package io.fineo.etl.spark;

import com.fasterxml.jackson.jr.ob.JSON;
import io.fineo.etl.spark.options.ETLOptions;
import io.fineo.etl.spark.read.DataFrameLoader;
import io.fineo.etl.spark.read.PartitionKey;
import io.fineo.etl.spark.util.FieldTranslatorFactory;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.E2ETestState;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.ITEndToEndLambdaLocal;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.etl.FineoProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.legacy.StreamType.ARCHIVE;
import static io.fineo.schema.avro.AvroSchemaEncoder.IS_BASE_FIELD;
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
  private static final Comparator<Map<String, Object>> EVENT_SORTER = (m1, m2) -> -(
    (Long) m1.get(TIMESTAMP_KEY)).compareTo((Long) m2.get(TIMESTAMP_KEY));
  private static final Predicate<String> ORG_OR_METRIC_KEY = fieldName ->
    fieldName.equals(ORG_ID_KEY) ||
    fieldName.equals(ORG_METRIC_TYPE_KEY);

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
    File archive = folder.newFolder("completed");
    Pair<E2ETestState, ETLOptions> ran = run(ingest, archive, null);
    verifyETLOutput(ran, ran.getLeft().getRunner().getProgress().getJson());
  }

  @Test
  public void testChangingSchemaForOrg() throws Exception {
    File lambdaOutput = folder.newFolder("lambda-output");
    File sparkOutput = folder.newFolder("spark-output");
    // run the basic test for the single record
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    Pair<E2ETestState, ETLOptions> ran = run(lambdaOutput, sparkOutput, json);
//    verifyETLOutput(ran, json);

    ETLOptions opts = ran.getRight();
    E2ETestState state = ran.getLeft();

    // add a field to the record, run the ingest again
    Map<String, Object> second = new HashMap<>(json);
    second.put(TIMESTAMP_KEY, System.currentTimeMillis() + 1);
    second.put("anotherField", 1);
    runIngest(state, lambdaOutput, second);

    // run ETL job
    SchemaStore store = state.getResources().getStore();
    runJob(opts, store);

    // make sure we can read both events
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
    E2ETestState state =
      runWithRecordsAndWriteToFile(lambdaOutput, records[0]);
    runIngest(state, lambdaOutput, records[1]);

    ETLOptions opts = getOpts(lambdaOutput, sparkOutput);
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
    E2ETestState state = ITEndToEndLambdaLocal.prepareTest();
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

    SchemaStore store = state.getResources().getStore();
    ETLOptions opts = getOpts(lambdaOutput, sparkOutput);
    runJob(opts, store);
    Map<String, Object> translated = translate(json, store);
    // verify that we wrote the rest of the field correctly
    DataFrameLoader loader = new DataFrameLoader(spark.jsc());
    Pair<PartitionKey, DataFrame> pair = loader.loadFrameForKnownSchema(opts.completed()).get(0);
    DataFrame frame = pair.getRight();
    for (Map.Entry<String, Object> entry : translated.entrySet()) {
      if (entry.getKey().equals(unknownFieldName)) {
        continue;
      }
      frame = where(frame, entry);
    }

    frame = select(frame, translated.keySet());
    List<Row> rows = frame.collectAsList();
    verifyMappedEvents(asRows(rows, pair), translated);

    // verify that we can read the hidden field correctly
    pair = loader.loadFrameForUnknownSchema(opts.completed()).get(0);
    frame = pair.getRight();
    String fieldKey = SparkETL.UNKNOWN_FIELDS_KEY + "." + unknownFieldName;
    // casting the unknown field to a an integer as well as reading it
    frame = frame.select(new Column(ORG_ID_KEY), new Column(ORG_METRIC_TYPE_KEY),
      new Column(fieldKey).cast(DataTypes.IntegerType));
    rows = frame.collectAsList();
    Map<String, Object> expected = new HashMap<>();
    expected.put(ORG_ID_KEY, translated.get(ORG_ID_KEY));
    expected.put(ORG_METRIC_TYPE_KEY, translated.get(ORG_METRIC_TYPE_KEY));
    expected.put(unknownFieldName, 1);
    verifyMappedEvents(asRows(rows, pair), expected);

    // verify that we can read both the known and unknown together
    pair = loader.loadFrameForKnownSchema(opts.completed()).get(0);
    DataFrame known = pair.getRight();
    Pair<PartitionKey, DataFrame> unknownPair =
      loader.loadFrameForUnknownSchema(opts.completed()).get(0);
    DataFrame unknown = unknownPair.getRight();
    DataFrame both = known.join(unknown,
      JavaConversions.asScalaBuffer(newArrayList(ORG_ID_KEY, ORG_METRIC_TYPE_KEY, TIMESTAMP_KEY)));
    String canonicalField =
      translated.keySet().stream().filter(IS_BASE_FIELD.negate()).findFirst()
                .get();
    rows = select(both, ORG_ID_KEY, ORG_METRIC_TYPE_KEY, TIMESTAMP_KEY, fieldKey, canonicalField)
      .collectAsList();
    expected.put(unknownFieldName, "1");
    expected.put(canonicalField, translated.get(canonicalField));
    verifyMappedEvents(asRows(rows, pair), expected);
  }

  private List<Pair<PartitionKey, List<Row>>> asRows(List<Row> rows,
    Pair<PartitionKey, DataFrame> pair) {
    return newArrayList(new ImmutablePair<>(pair.getKey(), rows));
  }

  private Pair<E2ETestState, ETLOptions> run(File ingest, File archive,
    Map<String, Object> json) throws Exception {
    // run the basic ingest job
    E2ETestState state = runWithRecordsAndWriteToFile(ingest, json);
    ETLOptions opts = getOpts(ingest, archive);
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

  private void runIngest(E2ETestState state, File lambdaOutput,
    Map<String, Object> record) throws Exception {
    ITEndToEndLambdaLocal.run(state, record);
    copyLamdaOutputToSparkInput(state, lambdaOutput);
    state.getRunner().cleanup();
  }

  private ETLOptions getOpts(File lambdaOutput, File sparkOutput) throws IOException {
    ETLOptions opts = new ETLOptions();
    File archiveOut = new File(sparkOutput, "output");
    File completed;
    try {
      completed = folder.newFolder("archiveDir");
    } catch (IOException e) {
      completed = new File(folder.getRoot(), "archiveDir");
    }
    String base = "file://";
    opts.source(base + lambdaOutput.getAbsolutePath());
    opts.completed(base + archiveOut.getAbsolutePath());
    opts.archiveDir(base + completed.getAbsolutePath());
    LOG.info("Lambda output: " + opts.source());
    LOG.info("Spark output: " + opts.completed());
    LOG.info("Completed files: " + opts.archiveDir());
    return opts;
  }

  private void verifyETLOutput(Pair<E2ETestState, ETLOptions> ran,
    Map<String, Object>... events)
    throws Exception {
    E2ETestState state = ran.getLeft();
    ETLOptions opts = ran.getRight();
    SchemaStore store = state.getResources().getStore();
    logEvents(store, events);

    // sort the events by timestamp to match how we read (sorted) which corresponds to creation
    // order
    List<Map<String, Object>> eventList = newArrayList(events);
    Collections.sort(eventList, EVENT_SORTER);

    events_loop:
    for (Map<String, Object> event : eventList) {
      Map<String, Object> mapped = translate(event, store);
      List<Pair<PartitionKey, DataFrame>> frames = getFrames(opts.completed());
      List<Pair<PartitionKey, List<Row>>> rows = new ArrayList<>();
      frame_loop:
      for (Pair<PartitionKey, DataFrame> pair : frames) {
        // skip this frame if we know it won't match the org and metric
        if (!(
          mapped.get(ORG_ID_KEY).equals(pair.getKey().getOrg()) && mapped.get
            (ORG_METRIC_TYPE_KEY).equals(pair.getKey().getMetricId()))) {
          continue;
        }

        DataFrame frame = pair.getRight();
        frame = select(frame, mapped.keySet());
        // map the fields to get exact matches for each row
        event_entry_loop:
        for (Map.Entry<String, Object> entry : mapped.entrySet()) {
          frame = where(frame, entry);
        }
        rows.add(new ImmutablePair<>(pair.getKey(), frame.collectAsList()));
      }
      verifyMappedEvents(rows, mapped);
    }

    List<Pair<PartitionKey, List<Row>>> rows = readAllRows(opts.completed());
    assertRowsEqualsRawEvents(rows, store, eventList.toArray(new Map[0]));
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
      String orgID = (String) event.get(ORG_ID_KEY);
      Metadata orgMeta = store.getOrgMetadata(orgID);
      Metric metric = store.getMetricMetadataFromAlias(orgMeta,
        (String) event.get(ORG_METRIC_TYPE_KEY));
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

  private List<Pair<PartitionKey, List<Row>>> readAllRows(String fileDir) throws Exception {
    return readAllRows(fileDir, "*");
  }

  private List<Pair<PartitionKey, List<Row>>> readAllRows(String fileDir, String... fields)
    throws IOException {
    // allow merging schema for parquet
    List<Pair<PartitionKey, List<Row>>> rows = new ArrayList<>();
    List<Pair<PartitionKey, DataFrame>> frames = getFrames(fileDir);
    for (int i = 0; i < frames.size(); i++) {
      String tableName = "table" + i;
      Pair<PartitionKey, DataFrame> pair = frames.get(i);
      DataFrame records = pair.getRight();
      records.registerTempTable(tableName);
      if (!fields[0].equals("*")) {
        records = select(records, fields);
      }
      records = records.sort(new Column(TIMESTAMP_KEY).desc());
      rows.add(new ImmutablePair<>(pair.getLeft(), records.collectAsList()));
    }
    return rows;
  }

  private DataFrame select(DataFrame records, String... fields) {
    return select(records, Arrays.asList(fields));
  }

  private DataFrame select(DataFrame records, Collection<String> fields) {
    LOG.info("Current frame schema: \n" + records.schema() + "\nRequesting fields: " + fields);
    return records.select(fields.stream()
                                .map(field -> new Column(field))
                                .collect(Collectors.toList())
                                .toArray(new Column[0]));
  }

  private List<Pair<PartitionKey, DataFrame>> getFrames(String fileDir) throws IOException {
    DataFrameLoader loader = new DataFrameLoader(spark.jsc());
    return loader.loadFrameForKnownSchema(fileDir);
  }

  private void assertRowsEqualsRawEvents(List<Pair<PartitionKey, List<Row>>> rows,
    SchemaStore store,
    Map<String, Object>... json) {
    verifyMappedEvents(rows,
      Arrays.asList(json)
            .stream()
            .map(event -> translate(event, store))
            .sorted(EVENT_SORTER)
            .collect(Collectors.toList())
            .toArray(new HashMap[0]));
  }

  private void verifyMappedEvents(List<Pair<PartitionKey, List<Row>>> rows, Map<String, Object>...
    mappedEvents) {
    List<Pair<PartitionKey, Row>> flatRows =
      rows.stream()
          .flatMap(pair -> pair.getRight().stream()
                               .map(row -> new ImmutablePair<>(pair.getLeft(), row)))
          .sorted((p1, p2) -> {
            Row r1 = p1.getRight();
            Row r2 = p2.getRight();
            int i1 = (Integer) r1.schema().getFieldIndex(TIMESTAMP_KEY).get();
            int i2 = (Integer) r2.schema().getFieldIndex(TIMESTAMP_KEY).get();
            return -Long.compare(r1.getLong(i1), r2.getLong(i2));
          })
          .collect(Collectors.toList());
    assertEquals("Got unexpected number of rows: " + rows, mappedEvents.length, flatRows.size());
    for (int i = 0; i < mappedEvents.length; i++) {
      Map<String, Object> fields = mappedEvents[i];

      Pair<PartitionKey, Row> pair = flatRows.get(i);
      PartitionKey partition = pair.getKey();
      // check the org/metric
      assertEquals("Got mismatch for org id!", fields.get(ORG_ID_KEY), partition.getOrg());
      assertEquals("Got mismatch for metric type!", fields.get(ORG_METRIC_TYPE_KEY),
        partition.getMetricId());

      Row row = pair.getRight();
      scala.collection.Map<String, Object> rowFields =
        row.getValuesMap(JavaConversions.asScalaBuffer(newArrayList(fields.keySet())));
      // number of fields - partition fields
      assertEquals(fields.size(), rowFields.size());
      for (String field : fields.keySet()) {
        Object value = fields.get(field);
        assertEquals("Mismatch for " + field + ". \nEvent: " + fields + "\nRow: " + row,
          value,
          rowFields.get(field).get());
      }
    }
  }

  public E2ETestState runWithRecordsAndWriteToFile(File outputDir,
    Map<String, Object> record)
    throws Exception {
    E2ETestState state = record == null ?
                         ITEndToEndLambdaLocal.runTest() :
                         ITEndToEndLambdaLocal.runTest(record);
    copyLamdaOutputToSparkInput(state, outputDir);
    state.getRunner().cleanup();
    return state;
  }

  private void copyLamdaOutputToSparkInput(E2ETestState state,
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
