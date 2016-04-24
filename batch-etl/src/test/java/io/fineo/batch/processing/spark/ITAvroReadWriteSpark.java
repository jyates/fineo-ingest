package io.fineo.batch.processing.spark;

import com.fasterxml.jackson.jr.ob.JSON;
import io.fineo.etl.spark.SparkETL;
import io.fineo.etl.spark.options.ETLOptions;
import io.fineo.etl.spark.read.DataFrameLoader;
import io.fineo.etl.spark.util.FieldTranslatorFactory;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.ITEndToEndLambdaLocal;
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
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
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
import static io.fineo.etl.FineoProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.legacy.StreamType.ARCHIVE;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_ID_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.ORG_METRIC_TYPE_KEY;
import static io.fineo.schema.avro.AvroSchemaEncoder.TIMESTAMP_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Simple test class that ensures we can read/write avro files from spark
 */
public class ITAvroReadWriteSpark {

  private static final Log LOG = LogFactory.getLog(ITAvroReadWriteSpark.class);
  private static final String DIR_PROPERTY = "fineo.spark.dir";

  @ClassRule
  public static LocalSparkRule spark = new LocalSparkRule();

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
    Pair<ITEndToEndLambdaLocal.TestState, ETLOptions> ran = run(ingest, archive, null);
    verifyETLOutput(ran, ran.getLeft().getRunner().getProgress().getJson());
  }

  @Test
  public void testChangingSchemaForOrg() throws Exception {
    File lambdaOutput = folder.newFolder("lambda-output");
    File sparkOutput = folder.newFolder("spark-output");
    // run the basic test for the single record
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    Pair<ITEndToEndLambdaLocal.TestState, ETLOptions> ran = run(lambdaOutput, sparkOutput, json);
    verifyETLOutput(ran, json);

    ETLOptions opts = ran.getRight();
    ITEndToEndLambdaLocal.TestState state = ran.getLeft();

    // add a field to the record, run it again
    Map<String, Object> second = new HashMap<>(json);
    second.put(AvroSchemaEncoder.TIMESTAMP_KEY, System.currentTimeMillis());
    second.put("anotherField", 1);

    // run again
    runIngest(state, lambdaOutput, second);

    // run ETL job
    SchemaStore store = state.getResources().getStore();
    runJob(opts, store);
    verifyETLOutput(ran, json, second);
  }

  @Test
  public void testIngestMultipleRecords() throws Exception {
    File lambdaOutput = folder.newFolder("lambda-output");
    String sparkOutputDir = System.getProperty(DIR_PROPERTY);
    File sparkOutput = sparkOutputDir != null ?
                       new File(sparkOutputDir) :
                       folder.newFolder("spark-output");
    Map<String, Object>[] records = LambdaTestUtils.createRecords(2, 1);
    ITEndToEndLambdaLocal.TestState state =
      runWithRecordsAndWriteToFile(lambdaOutput, records[0]);
    runIngest(state, lambdaOutput, records[1]);

    LambdaClientProperties props = state.getRunner().getProps();
    ETLOptions opts = getOpts(lambdaOutput, sparkOutput, props);
    SchemaStore store = state.getResources().getStore();
    runJob(opts, store);

    verifyETLOutput(new ImmutablePair<>(state, opts), records);

    // drill read verification happens in the drill-testi, which we cannot do here b/c of
    // jvm conflicts however, we do write the expected output so we can verify it later
    List<Map<String, Object>> events = new ArrayList<>();
    for (Map<String, Object> event : records) {
      Map<String, Object> translated = translate(event, store);
      events.add(translated);
    }
    JSON json = JSON.std;
    File info = new File(sparkOutput, "info.json");
    json.write(events, info);
    LOG.info(" ===> Test output stored at: " + sparkOutput);
  }

  @Test
  public void testUnknownFields() throws Exception {
    File lambdaOutput = folder.newFolder("lambda-output");
    File sparkOutput = folder.newFolder("spark-output");
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    ITEndToEndLambdaLocal.TestState state = ITEndToEndLambdaLocal.prepareTest();
    EndToEndTestRunner runner = state.getRunner();
    runner.setup();
    // register some know fields
    runner.register(json);

    // create an unknown field
    Map<String, Object> sent = new HashMap<>(json);
    String unknownFieldName = "jfield-new";
    sent.put(unknownFieldName, 1);
    runner.send(sent);

    runner.validate();
    copyLamdaOutputToSparkInput(state, lambdaOutput);
    state.getRunner().cleanup();

    LambdaClientProperties props = state.getRunner().getProps();
    SchemaStore store = state.getResources().getStore();
    ETLOptions opts = getOpts(lambdaOutput, sparkOutput, props);
    runJob(opts, store);
    Map<String, Object> translated = translate(json, store);
    // verify that we wrote the rest of the field correctly
    DataFrameLoader loader = new DataFrameLoader(spark.jsc());
    DataFrame frame = loader.loadFrameForKnownSchema(opts.archive());
    for (Map.Entry<String, Object> entry : translated.entrySet()) {
      if (entry.getKey().equals(unknownFieldName)) {
        continue;
      }
      frame = where(frame, entry);
    }

    frame = select(frame, translated.keySet());
    List<Row> rows = frame.collectAsList();
    verifyMappedEvents(rows, translated);

    // verify that we can read the hidden field correctly
    frame = loader.loadFrameForUnknownSchema(opts.archive());
    String fieldKey = SparkETL.UNKNOWN_FIELDS_KEY + "." + unknownFieldName;
    // casting the unknown field to a an integer as well as reading it
    frame = frame.select(new Column(ORG_ID_KEY), new Column(ORG_METRIC_TYPE_KEY),
      new Column(fieldKey).cast(DataTypes.IntegerType));
    rows = frame.collectAsList();
    Map<String, Object> expected = new HashMap<>();
    expected.put(ORG_ID_KEY, translated.get(ORG_ID_KEY));
    expected.put(ORG_METRIC_TYPE_KEY, translated.get(ORG_METRIC_TYPE_KEY));
    expected.put(unknownFieldName, 1);
    verifyMappedEvents(rows, expected);

    // verify that we can read both the known and unknown together
    DataFrame known = loader.loadFrameForKnownSchema(opts.archive());
    DataFrame unknown = loader.loadFrameForUnknownSchema(opts.archive());
    DataFrame both = known.join(unknown,
      JavaConversions.asScalaBuffer(newArrayList(ORG_ID_KEY, ORG_METRIC_TYPE_KEY, TIMESTAMP_KEY)));
    String canonicalField =
      translated.keySet().stream().filter(AvroSchemaEncoder.IS_BASE_FIELD.negate()).findFirst()
                .get();
    rows = select(both, ORG_ID_KEY, ORG_METRIC_TYPE_KEY, TIMESTAMP_KEY, fieldKey, canonicalField)
      .collectAsList();
    expected.put(unknownFieldName, "1");
    expected.put(canonicalField, translated.get(canonicalField));
    verifyMappedEvents(rows, expected);
  }

  private Pair<ITEndToEndLambdaLocal.TestState, ETLOptions> run(File ingest, File archive,
    Map<String, Object> json) throws Exception {
    // run the basic ingest job
    ITEndToEndLambdaLocal.TestState state = runWithRecordsAndWriteToFile(ingest, json);
    ETLOptions opts = getOpts(ingest, archive, state.getRunner().getProps());
    SchemaStore store = state.getResources().getStore();
    runJob(opts, store);
    return new ImmutablePair<>(state, opts);
  }

  private void runJob(ETLOptions opts, SchemaStore store) throws IOException, URISyntaxException {
    // run the spark job against the output from the ingest
    SparkETL etl = new SparkETL(opts);
    etl.run(spark.jsc(), store);

    // remove any previous history, so we don't try and read the old metastore
    cleanupMetastore();
  }

  private void runIngest(ITEndToEndLambdaLocal.TestState state, File lambdaOutput,
    Map<String, Object> record) throws Exception {
    ITEndToEndLambdaLocal.run(state, record);
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

  private void verifyETLOutput(Pair<ITEndToEndLambdaLocal.TestState, ETLOptions> ran,
    Map<String, Object>... events)
    throws Exception {
    ITEndToEndLambdaLocal.TestState state = ran.getLeft();
    ETLOptions opts = ran.getRight();
    SchemaStore store = state.getResources().getStore();
    logEvents(store, events);

    for (Map<String, Object> event : events) {
      Map<String, Object> mapped = translate(event, store);
      List<DataFrame> frames = getFrames(opts.archive());
      List<Row> rows = new ArrayList<>();
      for (DataFrame frame : frames) {
        frame = select(frame, mapped.keySet());
        // map the fields to get exact matches for each row
        for (Map.Entry<String, Object> entry : mapped.entrySet()) {
          frame = where(frame, entry);
        }
        rows.addAll(frame.collectAsList());
      }
      verifyMappedEvents(rows, mapped);
    }

    List<Row> rows = readAllRows(opts.archive());
    assertRowsEqualsRawEvents(rows, store, events);
    cleanupMetastore();
  }

  private DataFrame where(DataFrame frame, Map.Entry<String, Object> entry) {
    return frame.where(new Column(entry.getKey()).equalTo(entry.getValue()));
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
    DataFrameLoader loader = new DataFrameLoader(spark.jsc());
    return Arrays.asList(loader.loadFrameForKnownSchema(fileDir));
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

  public ITEndToEndLambdaLocal.TestState runWithRecordsAndWriteToFile(File outputDir,
    Map<String, Object> record)
    throws Exception {
    ITEndToEndLambdaLocal.TestState state = record == null ?
                                              ITEndToEndLambdaLocal.runTest() :
                                              ITEndToEndLambdaLocal.runTest(record);
    copyLamdaOutputToSparkInput(state, outputDir);
    state.getRunner().cleanup();
    return state;
  }

  private void copyLamdaOutputToSparkInput(ITEndToEndLambdaLocal.TestState state,
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
