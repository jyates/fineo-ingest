package io.fineo.etl.spark;

import com.google.common.collect.AbstractIterator;
import io.fineo.etl.options.ETLOptionBuilder;
import io.fineo.etl.options.ETLOptions;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;

public class SparkETL {

  public static final String DATE_KEY = "date";
  private final ETLOptions opts;

  public SparkETL(ETLOptions opts) {
    this.opts = opts;
  }

  public void run(JavaSparkContext context) throws URISyntaxException, IOException {
    // find all the files under the given root directory
    URI root = new URI(opts.root());
    FileSystem fs = FileSystem.get(root, context.hadoopConfiguration());
    Path rootPath = fs.resolvePath(new Path(root.getPath()));
    List<Path> sources = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(rootPath, true);
    while (iter.hasNext()) {
      LocatedFileStatus status = iter.next();
      if (!status.isDirectory()) {
        sources.add(status.getPath());
      }
    }

    // get each file in the staging area
    JavaPairRDD<String, PortableDataStream>[] stringRdds = new JavaPairRDD[sources.size()];
    for (int i = 0; i < sources.size(); i++) {
      stringRdds[i] = context.binaryFiles(sources.get(i).toString());
    }

    // transform each binary file into a bunch of avro generic records
    JavaRDD<GenericRecord>[] avroRecords = new JavaRDD[stringRdds.length];
    for (int i = 0; i < avroRecords.length; i++) {
      JavaPairRDD<String, PortableDataStream> content = stringRdds[i];
      avroRecords[i] = content.flatMap(new RecordMapper());
    }

    // combine and distinct the records
    JavaRDD<GenericRecord> records = context.union(avroRecords).distinct();

    // convert them into (org, metricId) -> records
    JavaPairRDD<Tuple2<String, String>, Iterable<GenericRecord>> typeToRecord =
      records.mapToPair(record -> {
        RecordMetadata metadata = RecordMetadata.get(record);
        return new Tuple2<>(new Tuple2<>(metadata.getOrgID(), metadata.getMetricCanonicalType()),
          record);
      }).groupByKey();

    // collect all the keys on THIS MACHINE. Its not completely scalable, but works enough #startup
    List<Tuple2<String, String>> types = typeToRecord.keys().collect();

    // get the schemas for each type
    LambdaClientProperties props = opts.props();
    SchemaStore store = props.createSchemaStore();

    List<Tuple3<JavaRDD<Row>, StructType, String>> schemas = new ArrayList<>();
    for (Tuple2<String, String> type : types) {
      JavaRDD<GenericRecord> grouped = getRddByKey(typeToRecord, type);
      Metric metric = store.getMetricMetadata(type._1(), type._2());
      String schemaString = metric.getMetricSchema();
      // parser keeps state and we redefine stuff, so we need to create a new one each time
      Schema.Parser parser = new Schema.Parser();
      Schema parsed = parser.parse(schemaString);
      Map<String, List<String>> canonicalToAliases = removeUnserializableAvroTypesFromMap(
        metric.getMetadata().getCanonicalNamesToAliases());
      JavaRDD<Row> rows =
        grouped.map(new RowConverter(schemaString, canonicalToAliases, type._1(), type._2()));

      // map each schema to a struct
      List<StructField> fields = new ArrayList<>();
      // base fields that we have to have in each record
      fields.add(
        DataTypes.createStructField(AvroSchemaEncoder.ORG_ID_KEY, DataTypes.StringType, false));
      fields.add(DataTypes
        .createStructField(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, DataTypes.StringType, false));
      fields.add(DataTypes.createStructField(DATE_KEY, DataTypes.DateType, false));
      fields.add(
        DataTypes.createStructField(AvroSchemaEncoder.TIMESTAMP_KEY, DataTypes.LongType, false));
      streamSchemaWithoutBaseFields(parsed)
        .forEach(field -> {
          fields.add(DataTypes.createStructField(field.name(), getSparkType(field), true));
        });
      schemas.add(
        new Tuple3<>(rows, DataTypes.createStructType(fields), metric.getMetadata().getVersion()));
    }

    // store the files by partition
    HiveContext sql = new HiveContext(context);
    schemas.stream().forEach(rowsAndSchemaAndSchemaVersion -> {
      sql.createDataFrame(rowsAndSchemaAndSchemaVersion._1(), rowsAndSchemaAndSchemaVersion._2())
         .write()
         .format("orc")
         .mode(SaveMode.Append)
         .partitionBy(AvroSchemaEncoder.ORG_ID_KEY, AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, DATE_KEY)
         .save(opts.archive() + "/" + rowsAndSchemaAndSchemaVersion._3());
    });

    archiveCompletedFiles(fs, sources, opts.completed());
  }

  private void archiveCompletedFiles(FileSystem fs, List<Path> sources, String archiveDir)
    throws IOException {
    Path archive = new Path(opts.completed(), Long.toString(System.currentTimeMillis()));
    boolean success = fs.mkdirs(archive);
    if (!success) {
      if (!fs.exists(archive)) {
        throw new IOException("Could not create completed archive directory:" + archive);
      }
    }
    for (Path source : sources) {
      Path target = new Path(archive, source.getName());
      if (!fs.rename(source, target)) {
        throw new IOException("Could not archive " + source + " -> " + target);
      }
    }
  }

  /**
   * We cannot serialize avro classes with the Java serializer, which is currently the only
   * serializer that is supported for serializing closures
   *
   * @param map multimap of string -> string
   * @return the same map, with as many of the same values as possible.
   */
  private Map<String, List<String>> removeUnserializableAvroTypesFromMap(
    Map<String, List<String>> map) {
    Map<String, List<String>> ret = new HashMap<>(map.size());
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      ret.put(entry.getKey(), newArrayList(entry.getValue()));
    }
    return ret;
  }

  private DataType getSparkType(Schema.Field field) {
    switch (field.schema().getType()) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case STRING:
        return DataTypes.StringType;
      case BYTES:
        return DataTypes.BinaryType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case DOUBLE:
        return DataTypes.DoubleType;
      default:
        throw new IllegalArgumentException("No spark type available for: " + field);
    }
  }

  static Stream<Schema.Field> streamSchemaWithoutBaseFields(Schema schema) {
    return schema.getFields().stream().sequential()
                 .filter(field -> !field.name().equals(AvroSchemaEncoder.BASE_FIELDS_KEY));
  }

  private <A, B> JavaRDD getRddByKey(JavaPairRDD<A, Iterable<B>> pairRDD, A key) {
    return pairRDD.filter(v -> v._1().equals(key)).values().flatMap(tuples -> tuples);
  }

  private static class RecordMapper
    implements FlatMapFunction<Tuple2<String, PortableDataStream>, GenericRecord> {
    @Override
    public Iterable<GenericRecord> call(
      Tuple2<String, PortableDataStream> tuple) throws Exception {
      PortableDataStream stream = tuple._2();
      FSDataInputStream in = (FSDataInputStream) stream.open();
      GenericRecordReader reader = new GenericRecordReader(in);
      return () -> reader;
    }
  }

  private static class GenericRecordReader extends AbstractIterator<GenericRecord> {

    private final FirehoseRecordReader<GenericRecord> reader;

    private GenericRecordReader(FSDataInputStream in) throws Exception {
      this.reader = FirehoseRecordReader.create(new AvroFSInput(in, in.available()));
    }

    @Override
    protected GenericRecord computeNext() {
      try {
        GenericRecord next = reader.next();
        if (next == null) {
          endOfData();
        }
        return next;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) throws URISyntaxException, IOException {
    // parse arguments
    ETLOptions opts = ETLOptionBuilder.build(args);
    if (opts.help() || opts.error()) {
      opts.printHelp();
      System.exit(opts.error() ? 1 : 0);
    }

    SparkETL etl = new SparkETL(opts);
    SparkConf conf = new SparkConf().setAppName(SparkETL.class.getName());
    final JavaSparkContext context = new JavaSparkContext(conf);
    etl.run(context);
  }
}
