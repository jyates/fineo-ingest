package io.fineo.etl.spark;

import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.base.Joiner;
import io.fineo.etl.FieldTranslatorFactory;
import io.fineo.etl.options.ETLOptions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.e2e.TestEndToEndLambdaLocal;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.configure.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties.StreamType.ARCHIVE;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Simple test class that ensures we can read/write avro files from spark
 */
public class TestAvroReadWriteSpark {

  private static final Log LOG = LogFactory.getLog(TestAvroReadWriteSpark.class);

  @ClassRule
  public static LocalSparkRule spark = new LocalSparkRule(conf -> {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "io.fineo.etl.AvroKyroRegistrator");
  });

  @Rule
  public TestOutput folder = new TestOutput(false);

  @After
  public void cleanupMetastore() throws IOException {
    File file = new File("metastore_db");
    FileUtils.deleteDirectory(file);
  }

  @Test
  public void testSingleRecord() throws Exception {
    File ingest = folder.newFolder("ingest");
    File archive = folder.newFolder("archive");
    Pair<TestEndToEndLambdaLocal.TestState, ETLOptions> ran = run(ingest, archive, null);
    verifyETLOutput(ran, ran.getLeft().getRunner().getProgress().getJson());
  }

  @Test
  public void testChangingSchemaForOrg() throws Exception {
    File lambdaOutput = folder.newFolder("lambda-output");
    File sparkOutput = folder.newFolder("spark-output");
    // run the basic test for the single record
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    Pair<TestEndToEndLambdaLocal.TestState, ETLOptions> ran = run(lambdaOutput, sparkOutput, json);
    verifyETLOutput(ran, json);

    ETLOptions opts = ran.getRight();
    TestEndToEndLambdaLocal.TestState state = ran.getLeft();

    // add a field to the record, run it again
    Map<String, Object> second = new HashMap<>(json);
    second.put(AvroSchemaEncoder.TIMESTAMP_KEY, System.currentTimeMillis());
    second.put("anotherField", 1);

    // run again
    runIngest(state, lambdaOutput, second);

    // run ETL job
    runJob(opts);
    verifyETLOutput(ran, json, second);
  }

  @Test
  public void testIngestMultipleRecords() throws Exception {
    File lambdaOutput = folder.newFolder("lambda-output");
    File sparkOutput = folder.newFolder("spark-output");
    Map<String, Object>[] records = LambdaTestUtils.createRecords(2, 1);
    TestEndToEndLambdaLocal.TestState state =
      runWithRecordsAndWriteToFile(lambdaOutput, records[0]);
    runIngest(state, lambdaOutput, records[1]);

    LambdaClientProperties props = state.getRunner().getProps();
    ETLOptions opts = getOpts(lambdaOutput, sparkOutput, props);
    runJob(opts);

    // verification happens in the drill testing, which we cannot do here b/c of jvm conflicts
    // however, we do write the expected output so we can verify it later
    logEvents(props.createSchemaStore(), records);
    FieldTranslatorFactory translatorFactory =
      new FieldTranslatorFactory(props.createSchemaStore());
    List<Map<String, Object>> events = new ArrayList<>();
    for (Map<String, Object> event : records) {
      String orgID = (String) event.get(AvroSchemaEncoder.ORG_ID_KEY);
      String metricAlias = (String) event.get(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY);
      FieldTranslatorFactory.FieldTranslator translator =
        translatorFactory.translate(orgID, metricAlias);
      Map<String, Object> translated = new HashMap<>();
      for (String field : event.keySet()) {
        Pair<String, Object> obj = translator.translate(field, event);
        translated.put(obj.getKey(), obj.getValue());
      }
      events.add(translated);
    }
    JSON json = JSON.std;
    File info = new File(sparkOutput, "info.json");
    json.write(events, info);
    LOG.info(" ===> Test output stored at: " + sparkOutput);
  }

  private void runIngest(TestEndToEndLambdaLocal.TestState state, File lambdaOutput,
    Map<String, Object> record) throws Exception {
    TestEndToEndLambdaLocal.run(state, record);
    copyLamdaOutputToSparkInput(state, lambdaOutput);
    state.getRunner().cleanup();
  }

  private ETLOptions getOpts(File lambdaOutput, File sparkOutput,
    LambdaClientProperties props) throws IOException {
    ETLOptions opts = new ETLOptions();
    File archiveOut = new File(sparkOutput, "output");
    File completed;
    try {
      completed = folder.newFolder("completed");
    } catch (IOException e) {
      completed = new File(folder.getRoot(), "completed");
    }
    String base = "file://";
    opts.archive(base + archiveOut.getAbsolutePath());
    opts.root(base + lambdaOutput.getAbsolutePath());
    opts.setCompletedDir(base + completed.getAbsolutePath());
    LOG.info("Lambda output: " + opts.root());
    LOG.info("Spark output: " + opts.archive());
    LOG.info("Completed files: " + opts.completed());

    opts.setProps(props);
    return opts;
  }

  private void verifyETLOutput(Pair<TestEndToEndLambdaLocal.TestState, ETLOptions> ran,
    Map<String, Object>... events)
    throws Exception {
    TestEndToEndLambdaLocal.TestState state = ran.getLeft();
    ETLOptions opts = ran.getRight();
    LambdaClientProperties props = state.getRunner().getProps();
    logEvents(props.createSchemaStore(), events);
    FieldTranslatorFactory translatorFactory =
      new FieldTranslatorFactory(props.createSchemaStore());
    for (Map<String, Object> event : events) {
      String orgID = (String) event.get(AvroSchemaEncoder.ORG_ID_KEY);
      String metricAlias = (String) event.get(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY);
      FieldTranslatorFactory.FieldTranslator translator =
        translatorFactory.translate(orgID, metricAlias);

      HiveContext sql = new HiveContext(spark.jsc());
      sql.setConf("spark.sql.orc.filterPushdown", "true");
      String dir = opts.archive() + "/0";
      LOG.info(" ==> Checking org " + orgID);
      DataFrame records = sql.read().format("orc").load(dir);
      records.registerTempTable("table");
      List<String> actualFields = event.keySet()
                                       .stream().
                                         map(field -> translator.translate(field))
                                       .collect(Collectors.toList());
      LOG.info("Getting fields: " + actualFields);
      String stmt =
        "SELECT " + (Joiner.on(',').join(actualFields)) + " FROM " + "table " +
        "WHERE " + AvroSchemaEncoder.ORG_ID_KEY + " = \"" + orgID + "\"";
      List<Row> rows = sql.sql(stmt).toJavaRDD().collect();
      LOG.info("For org: " + orgID + ", got rows: " + rows);
      cleanupMetastore();
    }
    List<Row> rows = readAllRows(opts.archive());
    assertRowsEqualsEvents(rows, props.createSchemaStore(), events);
    cleanupMetastore();
  }

  private void logEvents(SchemaStore store, Map<String, Object>... events) {
    LOG.info("===== Verifying Events ======");
    for (int i = 0; i < events.length; i++) {
      LOG.info(i + " =>");
      Map<String, Object> event = events[i];
      LOG.info("\t Raw => " + event);
      String orgID = (String) event.get(AvroSchemaEncoder.ORG_ID_KEY);
      Metadata orgMeta = store.getOrgMetadata(orgID);
      Metric metric = store.getMetricMetadataFromAlias(orgMeta,
        (String) event.get(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY));
      LOG.info("\t Org => " + orgMeta);
      LOG.info("\t Metric => " + metric);
    }
  }

  private void assertRowsEqualsEvents(List<Row> rows, SchemaStore store,
    Map<String, Object>... json) {
    assertEquals("Wrong number of rows! Got: " + rows, json.length, rows.size());
    // check the content against the object
    int i = 0;
    for (Map<String, Object> msg : json) {
      Row row = rows.get(i++);
      String orgId = (String) msg.get(ORG_ID_KEY);
      Map<String, Object> fields = new HashMap<>();
      fields.put(ORG_ID_KEY, orgId);
      Metadata metadata = store.getOrgMetadata(orgId);
      String metricName = metadata.getCanonicalNamesToAliases().keySet().iterator().next();
      Metric metric = store.getMetricMetadata(orgId, metricName);
      Map<String, String> aliasToCName = AvroSchemaManager.getAliasRemap(metric);
      fields.put(ORG_METRIC_TYPE_KEY, metricName);
      long ts = (long) msg.get(TIMESTAMP_KEY);
      fields.put(TIMESTAMP_KEY, ts);
      // add non-base fields
      for (String field : msg.keySet()) {
        if (AvroSchemaEncoder.IS_BASE_FIELD.negate().test(field)) {
          String cname = aliasToCName.get(field);
          fields.put(cname, msg.get(field));
        }
      }

      scala.collection.Map<String, Object> rowFields =
        row.getValuesMap(JavaConversions.asScalaBuffer(newArrayList(fields.keySet())));
      assertEquals(fields.size(), rowFields.size());
      for (Map.Entry<String, Object> field : fields.entrySet()) {
        assertEquals("Mismatch for " + field.getKey(), field.getValue(),
          rowFields.get(field.getKey()).get());
      }
    }
  }

  private Pair<TestEndToEndLambdaLocal.TestState, ETLOptions> run(File ingest, File archive,
    Map<String, Object> json) throws Exception {
    // run the basic ingest job
    TestEndToEndLambdaLocal.TestState state = runWithRecordsAndWriteToFile(ingest, json);
    ETLOptions opts = getOpts(ingest, archive, state.getRunner().getProps());
    runJob(opts);
    return new ImmutablePair<>(state, opts);
  }

  private void runJob(ETLOptions opts) throws IOException, URISyntaxException {
    // run the spark job against the output from the ingest
    SparkETL etl = new SparkETL(opts);
    etl.run(spark.jsc());

    // remove any previous history, so we don't try and read the old metastore
    cleanupMetastore();
  }

  private List<Row> readAllRows(String fileDir) throws Exception {
    // read all the rows that we stored
    HiveContext sql = new HiveContext(spark.jsc());
    sql.setConf("spark.sql.orc.filterPushdown", "true");
    // allow merging schema for parquet
    // sql.setConf(" spark.sql.parquet.mergeSchema", "true");
    FileSystem fs = FileSystem.get(spark.jsc().hadoopConfiguration());
    Path output = new Path(fileDir);
    FileStatus[] files = fs.listStatus(output);
    JavaRDD<Row>[] tables = new JavaRDD[files.length];
    for (int i = 0; i < files.length; i++) {
      FileStatus file = files[i];
      DataFrame records = sql.read().format("orc").load(file.getPath().toUri().toString());
      String tableName = "table" + i;
      records.registerTempTable(tableName);
      tables[i] = sql.sql("SELECT * FROM " + tableName).toJavaRDD();
    }

    return spark.jsc().union(tables).collect();
  }

  public TestEndToEndLambdaLocal.TestState runWithRecordsAndWriteToFile(File outputDir,
    Map<String, Object> record)
    throws Exception {
    TestEndToEndLambdaLocal.TestState state = record == null ?
                                              TestEndToEndLambdaLocal.runTest() :
                                              TestEndToEndLambdaLocal.runTest(record);
    copyLamdaOutputToSparkInput(state, outputDir);
    state.getRunner().cleanup();

    // reuse the same store from the test runner anytime we run again
    LambdaClientProperties props = state.getRunner().getProps();
    props.setStoreProvider(() -> state.getResources().getStore());
    return state;
  }

  private void copyLamdaOutputToSparkInput(TestEndToEndLambdaLocal.TestState state,
    File outputDir) throws IOException {
    // save the record(s) to a file
    File file = new File(outputDir, UUID.randomUUID().toString());
    File file2 = new File(outputDir, UUID.randomUUID().toString());

    LambdaClientProperties props = state.getRunner().getProps();
    String stream = props.getFirehoseStreamName(STAGED_PREFIX, ARCHIVE);
    ResourceManager resources = state.getResources();
    writeStreamToFile(resources, stream, file);
    writeStreamToFile(resources, stream, file2);
  }

  private void writeStreamToFile(ResourceManager resources, String stream, File file)
    throws IOException {
    FileOutputStream fout = new FileOutputStream(file);
    FileChannel channel = fout.getChannel();
    resources.getFirehoseWrites(stream).stream().forEach(buff -> {
      buff.flip();
      try {
        channel.write(buff);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    channel.close();

  }
}
