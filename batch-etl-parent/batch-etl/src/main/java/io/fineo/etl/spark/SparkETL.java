package io.fineo.etl.spark;

import com.google.common.base.Joiner;
import com.google.inject.Guice;
import io.fineo.etl.AvroKyroRegistrator;
import io.fineo.etl.spark.fs.FileCleaner;
import io.fineo.etl.spark.fs.RddLoader;
import io.fineo.etl.spark.options.ETLOptions;
import io.fineo.etl.spark.options.OptionsHandler;
import io.fineo.etl.spark.util.AvroSparkUtils;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.etl.spark.fs.RddUtils.getRddByKey;
import static io.fineo.etl.spark.util.AvroSparkUtils.getSparkType;
import static io.fineo.schema.store.AvroSchemaProperties.BASE_FIELDS_KEY;
import static io.fineo.schema.store.AvroSchemaProperties.ORG_ID_KEY;
import static io.fineo.schema.store.AvroSchemaProperties.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.store.AvroSchemaProperties.TIMESTAMP_KEY;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

public class SparkETL {

  public static final String FORMAT = "parquet";
  public static final String UNKNOWN_DATA_FORMAT = "json";
  // TODO really need to make a common protocol across drill and batch processing... #startup
  // this version matches the version used in Drill's read layout
  public static final String VERSION = "0";
  private final ETLOptions opts;
  public static String UNKNOWN_FIELDS_KEY = "unknown";
  private SchemaStore store;

  private static final Joiner PATH_PARTS = Joiner.on("/");

  public SparkETL(ETLOptions opts) {
    this.opts = opts;
  }

  public void run(JavaSparkContext context, SchemaStore store)
    throws URISyntaxException, IOException {
    // Read in all the raw json records
    this.store = store;
    RddLoader loader = new RddLoader(context, newArrayList(opts.source()));
    loader.load();
    JavaPairRDD<String, PortableDataStream>[] stringRdds = loader.getRdds();
    JavaRDD<GenericRecord> records = getRecords(context, stringRdds);

    // Convert them into (org, metricId, date) -> records
    JavaPairRDD<RecordKey, Iterable<GenericRecord>> typeToRecord =
      records.flatMapToPair(new RecordToKeyMapper()).groupByKey();

    // Collect all the keys (org, metric, date) on THIS MACHINE. Its not completely scalable, but
    // works enough #startup
    List<RecordKey> types = typeToRecord.keys().collect();

    SQLContext sql = new SQLContext(context);
    List<RecordKey> known = types.stream().filter(key -> !key.isUnknown()).collect(toList());
    Set<String> dirs = handleKnownFields(known, typeToRecord, sql);
    // sometimes we get empty files from the write, so remove those so drill is happy
    FileCleaner cleaner = new FileCleaner(loader.getFs());
    cleaner.clean(dirs, FileCleaner.PARQUET_MIN_SIZE);

    // Handle the unknown fields as simple json
    List<RecordKey> unknown = types.stream().filter(key -> key.isUnknown()).collect(toList());
    cleaner.clean(handleUnknownFields(unknown, typeToRecord, sql), FileCleaner.ZERO_LENGTH_FILES);

    loader.archive(opts.archiveDir());
  }

  private Set<String> handleKnownFields(List<RecordKey> known,
    JavaPairRDD<RecordKey, Iterable<GenericRecord>> typeToRecord, SQLContext sql) {
    // Remap the record by their type and into their appropriate partition
    List<Tuple2<RecordKey, Tuple2<JavaRDD<Row>, StructType>>> schemas =
      mapTypesAndGroupByPartition(known, typeToRecord);

    // store the files by partition
    Set<String> dirs = new HashSet<>();
    for (Tuple2<RecordKey, Tuple2<JavaRDD<Row>, StructType>> tuple : schemas) {
      RecordKey partitions = tuple._1();
      Tuple2<JavaRDD<Row>, StructType> rows = tuple._2();
      String dir = getOutputDir(partitions, FORMAT);
      dirs.add(dir);
      sql.createDataFrame(rows._1(), rows._2())
         .write()
         .format(FORMAT)
         .mode(SaveMode.Append)
         // partitioning doesn't work with drill reads (yet see DRILL-4615), so we manually
         // partition on org, metric and date (above)
         // .partitionBy(AvroSchemaEncoder.ORG_ID_KEY, AvroSchemaEncoder.ORG_METRIC_TYPE_KEY,
         // DATE_KEY)
         .save(dir);
    }
    return dirs;
  }

  private String getOutputDir(RecordKey partitions, String format) {
    return PATH_PARTS.join(opts.completed(), format,
      partitions.getOrgId(), partitions.getMetricId(), partitions.getDate());
  }

  /**
   * Take the possible RecordKeys (org, metric, date) and map the incoming pairRDD to a RDD[Row]
   * with a single type. Only known fields are included so we can conform to a fixed schema
   *
   * @param keys
   * @param keyToRecord
   * @return
   */
  private List<Tuple2<RecordKey, Tuple2<JavaRDD<Row>, StructType>>> mapTypesAndGroupByPartition(
    List<RecordKey> keys,
    JavaPairRDD<RecordKey, Iterable<GenericRecord>> keyToRecord) {
    // get the schemas for each type
    List<Tuple2<RecordKey, Tuple2<JavaRDD<Row>, StructType>>> schemas = new ArrayList<>();
    for (RecordKey type : keys) {
      JavaRDD<GenericRecord> grouped = getRddByKey(keyToRecord, type);
      String org = type.getOrgId();
      String metricId = type.getMetricId();
      StoreClerk clerk = new StoreClerk(store, org);
      StoreClerk.Metric metric = clerk.getMetricForCanonicalName(metricId);
      String schemaString = metric.getUnderlyingMetric().getMetricSchema();
      // parser keeps state and we redefine the logical name, so we need to create a new Parser
      // each time
      Schema.Parser parser = new Schema.Parser();
      Schema parsed = parser.parse(schemaString);
      Map<String, List<String>> fieldAliasMap = new HashMap<>();
      metric.getUserVisibleFields().stream().forEach(field -> {
        fieldAliasMap.put(field.getCname(), field.getAliases());
      });
      Map<String, List<String>> canonicalToAliases = AvroSparkUtils
        .removeUnserializableAvroTypesFromMap(fieldAliasMap);
      JavaRDD<Row> rows =
        grouped.map(new RowConverter(schemaString, canonicalToAliases, org, metricId));
      schemas.add(new Tuple2<>(type, new Tuple2<>(rows, mapSchemaToStruct(parsed))));
    }
    return schemas;
  }

  private Set<String> handleUnknownFields(List<RecordKey> unknown,
    JavaPairRDD<RecordKey, Iterable<GenericRecord>> typeToRecord, SQLContext sql) {
    // early exit, no unknown types
    if (unknown.isEmpty()) {
      return Collections.emptySet();
    }

    List<StructField> fields = new ArrayList<>();
    addBaseFields(fields);
    fields.add(createStructField(UNKNOWN_FIELDS_KEY,
      new MapType(DataTypes.StringType, DataTypes.StringType, false), false));
    StructType type = createStructType(fields);
    Set<String> dirs = new HashSet<>();
    for (RecordKey key : unknown) {
      JavaRDD<GenericRecord> records = getRddByKey(typeToRecord, key);
      JavaRDD<Row> rows = records.map(record -> {
        RecordMetadata metadata = RecordMetadata.get(record);
        return RowFactory.create(metadata.getOrgID(), metadata.getMetricCanonicalType(),
          metadata.getBaseFields().getTimestamp(), metadata.getBaseFields().getUnknownFields());
      });
      String dir = getOutputDir(key, UNKNOWN_DATA_FORMAT);
      dirs.add(dir);
      sql.createDataFrame(rows, type)
         .write()
         .format(UNKNOWN_DATA_FORMAT)
         .mode(SaveMode.Append)
         .save(dir);
    }
    return dirs;
  }

  private StructType mapSchemaToStruct(Schema parsed) {
    List<StructField> fields = new ArrayList<>();
    addBaseFields(fields);
    streamSchemaWithoutBaseFields(parsed)
      .forEach(field -> {
        Schema fieldSchema = field.schema();
        List<Schema.Field> schemaFields = fieldSchema.getFields();
        fields.add(createStructField(field.name(), getSparkType(schemaFields.get(1)), true));
      });
    return createStructType(fields);
  }

  private void addBaseFields(List<StructField> fields) {
    fields.add(createStructField(ORG_ID_KEY, DataTypes.StringType, false));
    fields.add(createStructField(ORG_METRIC_TYPE_KEY, DataTypes.StringType, false));
    // need to include timestamp since that is _different_ (more specific) than the date range
    fields.add(createStructField(TIMESTAMP_KEY, DataTypes.LongType, false));
  }

  private JavaRDD<GenericRecord> getRecords(JavaSparkContext context,
    JavaPairRDD<String, PortableDataStream>[] stringRdds) {
    // transform each binary file into a bunch of avro generic records
    JavaRDD<GenericRecord>[] avroRecords = new JavaRDD[stringRdds.length];
    for (int i = 0; i < avroRecords.length; i++) {
      JavaPairRDD<String, PortableDataStream> content = stringRdds[i];
      avroRecords[i] = content.flatMap(new RecordExtractor());
    }

    // combine and distinct the records
    return context.union(avroRecords).distinct();
  }

  static Stream<Schema.Field> streamSchemaWithoutBaseFields(Schema schema) {
    return schema.getFields()
                 .stream().sequential()
                 .filter(field -> !field.name().equals(BASE_FIELDS_KEY));
  }

  public static void main(String[] args) throws URISyntaxException, IOException {
    // parse arguments
    ETLOptions opts = OptionsHandler.handle(args);
    Properties props = PropertiesLoaderUtil.load();

    SchemaStore store = Guice.createInjector(
      new PropertiesModule(props),
      new DynamoModule(),
      new DynamoRegionConfigurator(),
      new SchemaStoreModule()
    ).getInstance(SchemaStore.class);

    run(opts, store);
  }

  public static void run(ETLOptions opts, SchemaStore store)
    throws IOException, URISyntaxException {
    SparkETL etl = new SparkETL(opts);
    SparkConf conf = new SparkConf().setAppName(SparkETL.class.getName());
    conf.set("spark.serializer", KryoSerializer.class.getName());
    conf.set("spark.kryo.registrator", AvroKyroRegistrator.class.getName());
    final JavaSparkContext context = new JavaSparkContext(conf);
    etl.run(context, store);
  }
}
