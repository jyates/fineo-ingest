package io.fineo.etl.spark;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import io.fineo.etl.options.ETLOptions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.e2e.TestEndToEndLambdaLocal;
import io.fineo.lambda.e2e.TestOutput;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.etl.spark.SparkETL.DATE_KEY;
import static io.fineo.lambda.configure.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties.StreamType.ARCHIVE;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Simple test class that ensures we can read/write avro files from spark
 */
public class TestAvroReadWriteSpark extends SharedJavaSparkContext {

  private static final Log LOG = LogFactory.getLog(TestAvroReadWriteSpark.class);

  @Rule
  public TestOutput folder = new TestOutput(false);

  @Before
  public void runBefore() {
    conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "io.fineo.etl.AvroKyroRegistrator");
    super.runBefore();
  }

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
    TestEndToEndLambdaLocal.TestState state = ran.getLeft();
    ETLOptions opts = ran.getRight();
    LambdaClientProperties props = state.getRunner().getProps();
    Row[] rows = readAllRows(opts.archive());
    assertRowsEqualsEvents(rows, props.createSchemaStore(),
      state.getRunner().getProgress().getJson());
  }

  @Test
  public void testChangingSchemaForOrg() throws Exception {
    File lambdaOutput = folder.newFolder("lambda-output");
    File sparkOutput = folder.newFolder("spark-output");
    // run the basic test for the single record
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    Pair<TestEndToEndLambdaLocal.TestState, ETLOptions> ran = run(lambdaOutput, sparkOutput, json);
    ETLOptions opts = ran.getRight();
    Row[] rows = readAllRows(opts.archive());
    TestEndToEndLambdaLocal.TestState state = ran.getLeft();
    LambdaClientProperties props = state.getRunner().getProps();
    SchemaStore store = props.createSchemaStore();
    assertRowsEqualsEvents(rows, store, json);

    // add a field to the record, run it again
    Map<String, Object> second = new HashMap<>(json);
    second.put("anotherField", 1);

    // prepare for next iteration
    state.getResources().reset();

    // run again
    TestEndToEndLambdaLocal.run(state, second);
    runJob(opts);

    // verify that we get two records
    rows = readAllRows(opts.archive());
    assertRowsEqualsEvents(rows, store, json, second);
  }

  private ETLOptions getOpts(File lambdaOutput, File sparkOutput) throws IOException {
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
    LOG.info("Lambda output: "+opts.root());
    LOG.info("Spark output: "+opts.archive());
    LOG.info("Completed files: "+opts.completed());
    return opts;
  }

  private void assertRowsEqualsEvents(Row[] rows, SchemaStore store, Map<String, Object>... json) {
    assertEquals("Wrong number of rows! Got: " + Arrays.toString(rows), json.length, rows.length);
    // check the content against the object
    int i = 0;
    for (Map<String, Object> msg : json) {
      Row row = rows[i++];
      String orgId = (String) msg.get(ORG_ID_KEY);
      Map<String, Object> fields = new HashMap<>();
      fields.put(ORG_ID_KEY, orgId);
      Metadata metadata = store.getSchemaTypes(orgId);
      String metricName = metadata.getCanonicalNamesToAliases().keySet().iterator().next();
      Metric metric = store.getMetricMetadata(orgId, metricName);
      Map<String, String> aliasToCName = AvroSchemaManager.getAliasRemap(metric);
      fields.put(ORG_METRIC_TYPE_KEY, metricName);
      long ts = (long) msg.get(TIMESTAMP_KEY);
      fields.put(TIMESTAMP_KEY, ts);
      fields.put(DATE_KEY, new Date(ts).toString());
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
    Map<String, Object> json)
    throws Exception {
    ETLOptions opts = getOpts(ingest, archive);

    // run the basic ingest job
    TestEndToEndLambdaLocal.TestState state = runWithRecordsAndWriteToFile(ingest, json);
    LambdaClientProperties props = state.getRunner().getProps();

    // reuse the same store from the test runner
    props.setStoreProvider(() -> state.getResources().getStore());
    opts.setProps(props);

    runJob(opts);
    return new ImmutablePair<>(state, opts);
  }

  private void runJob(ETLOptions opts) throws IOException, URISyntaxException {
    // run the spark job against the output from the ingest
    SparkETL etl = new SparkETL(opts);
    etl.run(jsc());

    // remove any previous history, so we don't try and read the old metastore
    cleanupMetastore();
  }

  private Row[] readAllRows(String fileDir) throws Exception {
    // read all the rows that we stored
    HiveContext sql = new HiveContext(jsc());
    DataFrame records = sql.read().format("orc").load(fileDir);
    records.registerTempTable("table");
    return sql.sql("SELECT * FROM table").collect();
  }

  public TestEndToEndLambdaLocal.TestState runWithRecordsAndWriteToFile(File outputDir)
    throws Exception {
    return runWithRecordsAndWriteToFile(outputDir, null);
  }

  public TestEndToEndLambdaLocal.TestState runWithRecordsAndWriteToFile(File outputDir,
    Map<String, Object> record)
    throws Exception {
    TestEndToEndLambdaLocal.TestState state = record == null ?
                                              TestEndToEndLambdaLocal.runTest() :
                                              TestEndToEndLambdaLocal.runTest(record);
    ResourceManager resources = state.getResources();

    // save the record(s) to a file
    File file = new File(outputDir, UUID.randomUUID().toString());
    File file2 = new File(outputDir, UUID.randomUUID().toString());

    LambdaClientProperties props = state.getRunner().getProps();
    String stream = props.getFirehoseStreamName(STAGED_PREFIX, ARCHIVE);
    writeStreamToFile(resources, stream, file);
    writeStreamToFile(resources, stream, file2);
    return state;
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
