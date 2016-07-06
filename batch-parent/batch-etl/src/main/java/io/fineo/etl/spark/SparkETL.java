package io.fineo.etl.spark;

import com.google.inject.Guice;
import io.fineo.etl.spark.fs.FileCleaner;
import io.fineo.etl.spark.fs.RddLoader;
import io.fineo.etl.spark.options.ETLOptionBuilder;
import io.fineo.etl.spark.options.ETLOptions;
import io.fineo.etl.spark.util.AvroSparkUtils;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.SchemaStoreModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.etl.spark.util.AvroSparkUtils.getSparkType;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

public class SparkETL {

  public static final String FORMAT = "parquet";
  public static final String UNKNOWN_DATA_FORMAT = "json";
  private final ETLOptions opts;
  public static String UNKNOWN_FIELDS_KEY = "unknown";
  private SchemaStore store;

  public SparkETL(ETLOptions opts) {
    this.opts = opts;
  }

  public void run(JavaSparkContext context, SchemaStore store) throws URISyntaxException, IOException {
    this.store = store;
    RddLoader loader = new RddLoader(context, newArrayList(opts.root()));
    loader.load();
    JavaPairRDD<String, PortableDataStream>[] stringRdds = loader.getRdds();
    JavaRDD<GenericRecord> records = getRecords(context, stringRdds);

    // convert them into (org, metricId, date) -> records
    JavaPairRDD<RecordKey, Iterable<GenericRecord>> typeToRecord =
      records.flatMapToPair(new RecordToKeyMapper()).groupByKey();

    // collect all the keys on THIS MACHINE. Its not completely scalable, but works enough #startup
    List<RecordKey> types = typeToRecord.keys().collect();

    List<RecordKey> known = types.stream().filter(key -> !key.isUnknown()).collect(toList());
    List<Tuple3<JavaRDD<Row>, StructType, Date>> schemas =
      mapRecordsToTypesAndDay(known, typeToRecord);

    // store the files by partition
    SQLContext sql = new SQLContext(context);
    Set<String> dirs = new HashSet<>();
    for (Tuple3<JavaRDD<Row>, StructType, Date> tuple : schemas) {
      String dir = opts.archive() + "/" + FORMAT + "/" + tuple._3().toString();
      dirs.add(dir);
      sql.createDataFrame(tuple._1(), tuple._2())
         .write()
         .format(FORMAT)
        .mode(SaveMode.Append)
          // partitioning doesn't work well with drill reads, so we manually partition by date
//      .partitionBy(AvroSchemaEncoder.ORG_ID_KEY, AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, DATE_KEY)
        .save(dir);
    }
    FileCleaner cleaner = new FileCleaner(loader.getFs());
    cleaner.clean(dirs, FileCleaner.PARQUET_MIN_SIZE);

    List<RecordKey> unknown = types.stream().filter(key -> key.isUnknown()).collect(toList());
    cleaner.clean(handleUnknownFields(unknown, typeToRecord, sql), FileCleaner.ZERO_LENGTH_FILES);

    loader.archive(opts.completed());
  }

  private List<Tuple3<JavaRDD<Row>, StructType, Date>> mapRecordsToTypesAndDay(
    List<RecordKey> types,
    JavaPairRDD<RecordKey, Iterable<GenericRecord>> typeToRecord) {
    // get the schemas for each type
    List<Tuple3<JavaRDD<Row>, StructType, Date>> schemas = new ArrayList<>();
    for (RecordKey type : types) {
      JavaRDD<GenericRecord> grouped = getRddByKey(typeToRecord, type);
      String org = type.getOrgId();
      String metricId = type.getMetricId();
      Metric metric = store.getMetricMetadata(org, metricId);
      String schemaString = metric.getMetricSchema();
      // parser keeps state and we redefine stuff, so we need to create a new one each time
      Schema.Parser parser = new Schema.Parser();
      Schema parsed = parser.parse(schemaString);
      Map<String, List<String>> canonicalToAliases = AvroSparkUtils
        .removeUnserializableAvroTypesFromMap(
          metric.getMetadata().getCanonicalNamesToAliases());
      JavaRDD<Row> rows =
        grouped.map(new RowConverter(schemaString, canonicalToAliases, org, metricId));
      schemas.add(new Tuple3<>(rows, mapSchemaToStruct(parsed), type.getDate()));
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
      String dir = opts.archive() + "/" + UNKNOWN_DATA_FORMAT + "/" + key.getDate();
      dirs.add(dir);
      sql.createDataFrame(rows, type).write().format("json").mode(SaveMode.Append).save(dir);
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
    fields.add(
      createStructField(AvroSchemaEncoder.ORG_ID_KEY, DataTypes.StringType, false));
    fields.add(
      createStructField(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, DataTypes.StringType, false));
    fields.add(
      createStructField(AvroSchemaEncoder.TIMESTAMP_KEY, DataTypes.LongType, false));
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
                 .filter(field -> !field.name().equals(AvroSchemaEncoder.BASE_FIELDS_KEY));
  }

  private <A, B> JavaRDD getRddByKey(JavaPairRDD<A, Iterable<B>> pairRDD, A key) {
    return pairRDD.filter(v -> v._1().equals(key)).values().flatMap(tuples -> tuples);
  }

  public static void main(String[] args) throws URISyntaxException, IOException {
    // parse arguments
    ETLOptions opts = ETLOptionBuilder.build(args);
    if (opts.help() || opts.error()) {
      opts.printHelp();
      System.exit(opts.error() ? 1 : 0);
    }

    SchemaStore store = Guice.createInjector(
      new PropertiesModule(),
      new DynamoModule(),
      new DynamoRegionConfigurator(),
      new SchemaStoreModule()
    ).getInstance(SchemaStore.class);

    SparkETL etl = new SparkETL(opts);
    SparkConf conf = new SparkConf().setAppName(SparkETL.class.getName());
    final JavaSparkContext context = new JavaSparkContext(conf);
    etl.run(context, store);
  }
}
