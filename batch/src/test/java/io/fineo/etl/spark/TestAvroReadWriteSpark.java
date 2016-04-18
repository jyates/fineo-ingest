package io.fineo.etl.spark;

import com.fasterxml.jackson.jr.ob.JSON;
import io.fineo.etl.FieldTranslatorFactory;
import io.fineo.etl.options.ETLOptions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.e2e.TestEndToEndLambdaLocal;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.AvroSchemaEncoder;
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
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
    SchemaStore store = props.createSchemaStore();
    logEvents(store, records);
    List<Map<String, Object>> events = new ArrayList<>();
    for (Map<String, Object> event : records) {
      Map<String, Object> translated = translate(event, store);
      events.add(translated);
    }
    JSON json = JSON.std;
    File info = new File(sparkOutput, "info.json");
    json.write(events, info);
    LOG.info(" ===> Test output stored at: " + sparkOutput);
    verifyETLOutput(new ImmutablePair<>(state, opts), records);
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
    SchemaStore store = props.createSchemaStore();
    logEvents(store, events);

    for (Map<String, Object> event : events) {
      Map<String, Object> mapped = translate(event, store);
      List<DataFrame> frames = getFrames(opts.archive());
      List<Row> rows = new ArrayList<>();
      for (DataFrame frame : frames) {
        frame = select(frame, mapped.keySet());
        // map the fields to get exact matches for each row
        for (Map.Entry<String, Object> entry : mapped.entrySet()) {
          frame = frame.where(new Column(entry.getKey()).equalTo(entry.getValue()));
        }
        rows.addAll(frame.collectAsList());
      }
      verifyMappedEvents(rows, mapped);
    }

    List<Row> rows = readAllRows(opts.archive());
    assertRowsEqualsRawEvents(rows, props.createSchemaStore(), events);
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

  private Map<String, Object> translate(Map<String, Object> msg, SchemaStore store) {
    FieldTranslatorFactory factory = new FieldTranslatorFactory(store);
    FieldTranslatorFactory.FieldTranslator translator = translator(msg, factory);
    Map<String, Object> fields = new HashMap<>();
    msg.keySet().stream().forEach(key -> {
      Pair<String, Object> p = translator.translate(key, msg);
      fields.put(p.getKey(), p.getValue());
    });
    return fields;
  }

  public FieldTranslatorFactory.FieldTranslator translator(Map<String, Object> event,
    FieldTranslatorFactory factory) {
    String orgId = (String) event.get(ORG_ID_KEY);
    String metricAlias = (String) event.get(ORG_METRIC_TYPE_KEY);
    return factory.translate(orgId, metricAlias);
  }

  private List<Row> readAllRows(String fileDir) throws Exception {
    return readAllRows(fileDir, "*");
  }

  private List<Row> readAllRows(String fileDir, String... fields) throws IOException {
    // allow merging schema for parquet
    List<Row> rows = new ArrayList<>();
    List<DataFrame> frames = getFrames(fileDir);
    for (int i = 0; i < frames.size(); i++) {
      String tableName = "table" + i;
      DataFrame records = frames.get(i);
      records.registerTempTable(tableName);
      if (!fields[0].equals("*")) {
        records = select(records, fields);
      }
      records = records.sort(new Column(TIMESTAMP_KEY).asc());
      rows.addAll(records.collectAsList());
    }
    return rows;
  }

  private DataFrame select(DataFrame records, String... fields) {
    return select(records, Arrays.asList(fields));
  }

  private DataFrame select(DataFrame records, Collection<String> fields) {
    return records.select(fields.stream()
                                .map(field -> new Column(field))
                                .collect(Collectors.toList())
                                .toArray(new Column[0]));
  }

  private List<DataFrame> getFrames(String fileDir) throws IOException {
    SQLContext sql = new SQLContext(spark.jsc());
    sql.setConf("spark.sql.parquet.mergeSchema", "true");
    FileSystem fs = FileSystem.get(spark.jsc().hadoopConfiguration());
    Path output = new Path(fileDir);
    FileStatus[] files = fs.listStatus(output);
    List<DataFrame> frames = new ArrayList<>();
    for (int i = 0; i < files.length; i++) {
      FileStatus file = files[i];
      frames.add(sql.read().format("parquet").load(file.getPath().toUri().toString()));
    }
    return frames;
  }

  private void assertRowsEqualsRawEvents(List<Row> rows, SchemaStore store,
    Map<String, Object>... json) {
    verifyMappedEvents(rows,
      Arrays.asList(json)
            .stream()
            .map(event -> translate(event, store))
            .collect(Collectors.toList())
            .toArray(new HashMap[0]));
  }

  private void verifyMappedEvents(List<Row> rows, Map<String, Object>... mappedEvents) {
    assertEquals("Got unexpected number of rows: " + rows, mappedEvents.length, rows.size());

    for (int i = 0; i < mappedEvents.length; i++) {
      Map<String, Object> fields = mappedEvents[i];
      Row row = rows.get(i);
      scala.collection.Map<String, Object> rowFields =
        row.getValuesMap(JavaConversions.asScalaBuffer(newArrayList(fields.keySet())));
      assertEquals(fields.size(), rowFields.size());
      for (Map.Entry<String, Object> field : fields.entrySet()) {
        assertEquals("Mismatch for " + field.getKey() + ". \nEvent: " + fields + "\nRow: " + row,
          field.getValue(),
          rowFields.get(field.getKey()).get());
      }
    }
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
