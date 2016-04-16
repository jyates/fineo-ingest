package io.fineo.lambda.e2e;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.lambda.util.SchemaUtil;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.avro.TestRecordMetadata;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.fineo.lambda.configure.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties.StreamType.ARCHIVE;
import static io.fineo.lambda.configure.LambdaClientProperties.StreamType.COMMIT_ERROR;
import static io.fineo.lambda.configure.LambdaClientProperties.StreamType.PROCESSING_ERROR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Helper utility to implement an end-to-end test of the lambda architecture
 */
public class EndToEndTestRunner {

  private static final Log LOG = LogFactory.getLog(EndToEndTestRunner.class);

  private final LambdaClientProperties props;
  private final ResourceManager manager;
  private ProgressTracker progress;
  private final EndtoEndSuccessStatus status;

  public EndToEndTestRunner(LambdaClientProperties props, ResourceManager manager)
    throws Exception {
    this.props = props;
    this.manager = manager;
    this.status = new EndtoEndSuccessStatus();
  }

  public static void updateSchemaStore(SchemaStore store, Map<String, Object> event)
    throws Exception {
    String orgId = (String) event.get(AvroSchemaEncoder.ORG_ID_KEY);
    String metricType = (String) event.get(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY);
    Preconditions.checkArgument(orgId != null && metricType != null);
    // collect the fields that are not the base fields
    List<String> otherFields = event.keySet().stream().filter(AvroSchemaEncoder
      .IS_BASE_FIELD.negate()).collect(Collectors.toList());
    try {
      SchemaTestUtils.addNewOrg(store, orgId, metricType, otherFields.toArray(new String[0]));
    } catch (IllegalStateException e) {
      // need to update the schema for the org
      Metadata metadata = store.getOrgMetadata(orgId);
      Metric metric = store.getMetricMetadataFromAlias(metadata, metricType);
      SchemaBuilder builder = SchemaBuilder.create();
      SchemaBuilder.MetricBuilder metricBuilder = builder.updateOrg(metadata).updateSchema(metric);
      event.entrySet().stream().sequential()
           .filter(entry -> AvroSchemaEncoder.IS_BASE_FIELD.negate().test(entry.getKey()))
           .filter(entry -> !collectMapListValues(metric.getMetadata().getCanonicalNamesToAliases())
             .contains(entry.getKey()))
           .forEach(entry -> {
             String clazz = entry.getValue().getClass().getSimpleName().toUpperCase();
             if (clazz.equals("BYTE[]")) {
               metricBuilder.withBytes(entry.getKey()).asField();
               return;
             } else if (clazz.equals("INTEGER")) {
               metricBuilder.withInt(entry.getKey()).asField();
               return;
             }
             Schema.Type type = Schema.Type.valueOf(clazz);
             switch (type) {
               case BOOLEAN:
                 metricBuilder.withBoolean(entry.getKey()).asField();
                 return;
               case LONG:
                 metricBuilder.withLong(entry.getKey()).asField();
                 return;
               case FLOAT:
                 metricBuilder.withFloat(entry.getKey()).asField();
                 return;
               case DOUBLE:
                 metricBuilder.withDouble(entry.getKey()).asField();
                 return;
               case STRING:
                 metricBuilder.withString(entry.getKey()).asField();
                 return;
             }
           });

      store.updateOrgMetric(metricBuilder.build().build(), metric);
    }
  }


  private static <T> List<T> collectMapListValues(Map<?, List<T>> map) {
    return map.values().stream()
              .sequential()
              .flatMap(list -> list.stream())
              .collect(Collectors.toList());
  }


  public void setup() throws Exception {
    this.manager.setup(props);
    this.progress = new ProgressTracker(manager.getStore());
  }

  public void run(Map<String, Object> json) throws Exception {
    progress.sending(json);

    updateSchemaStore(progress.store, json);
    this.status.updated();

    this.progress.sent(this.manager.send(json));
    this.status.sent();
  }

  public void validate() throws Exception {
    validateRawRecordToAvro();
    status.rawToAvroPassed();

    validateAvroToStorage();
    status.avroToStoragePassed();

    status.success();
  }

  private void validateRawRecordToAvro() throws IOException {
    List<ByteBuffer> archived = manager.getFirehoseWrites(props.getFirehoseStreamName(RAW_PREFIX,
      ARCHIVE));
    ByteBuffer data = combine(archived);

    // ensure the bytes match from the archived/sent
    String expected = new String(progress.sent);
    String actual = new String(data.array());
    assertArrayEquals("Validating archived vs stored data in raw -> avro\n" +
                      "---- Raw data ->\n[" + expected + "]\n" +
                      "---- Archive  -> \n[" + actual + "]\n...don't match",
      progress.sent, data.array());

    String stream = props.getRawToStagedKinesisStreamName();
    verifyAvroRecordsFromStream(stream, () -> manager.getKinesisWrites(stream));

    // ensure that we didn't write any errors in the first stage
    verifyNoStageErrors(RAW_PREFIX, bbs -> new String(combine(bbs).array()));
  }

  private void validateAvroToStorage() throws IOException {
    verifyNoStageErrors(STAGED_PREFIX, bbs -> {
      try {
        return SchemaUtil.toString(LambdaTestUtils.readRecords(combine(bbs)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    // archive should be exactly the avro formatted json record
    String stream = props.getFirehoseStreamName(STAGED_PREFIX, ARCHIVE);
    verifyAvroRecordsFromStream(stream, () -> manager.getFirehoseWrites(stream));
    status.firehoseStreamCorrect(STAGED_PREFIX, ARCHIVE);

    // verify that we wrote the right things to DynamoDB
    RecordMetadata metadata = RecordMetadata.get(progress.avro);
    manager.verifyDynamoWrites(metadata, progress.json);
  }

  private void verifyAvroRecordsFromStream(String stream, Supplier<List<ByteBuffer>> bytes)
    throws IOException {
    List<ByteBuffer> parsedBytes = bytes.get();
    // read the parsed avro records
    List<GenericRecord> parsedRecords = LambdaTestUtils.readRecords(combine(parsedBytes));
    assertEquals("[" + stream + "] Got unexpected number of records: " + parsedRecords, 1,
      parsedRecords.size());
    GenericRecord record = parsedRecords.get(0);

    // org/schema naming
    TestRecordMetadata.verifyRecordMetadataMatchesExpectedNaming(record);
    verifyRecordMatchesJson(progress.store, progress.json, record);
    progress.avro = record;
  }

  private void verifyNoStageErrors(String stage, Function<List<ByteBuffer>, String> errorResult) {
    LOG.info("Checking to make sure that there are no errors in stage: " + stage);
    verifyNoFirehoseWrites(errorResult, stage, PROCESSING_ERROR, COMMIT_ERROR);
  }

  private void verifyNoFirehoseWrites(Function<List<ByteBuffer>, String> errorResult, String stage,
    LambdaClientProperties.StreamType... streams) {
    for (LambdaClientProperties.StreamType stream : streams) {
      LOG.debug("Checking stream: " + stage + "-" + stream + " has no writes...");
      empty(errorResult, manager.getFirehoseWrites(props.getFirehoseStreamName(stage, stream)));
      LOG.debug("Marking stream: " + stage + "-" + stream + " correct");
      status.firehoseStreamCorrect(stage, stream);
    }
  }

  private ByteBuffer combine(List<ByteBuffer> data) {
    int size = data.stream().mapToInt(bb -> bb.remaining()).sum();
    ByteBuffer combined = ByteBuffer.allocate(size);
    data.forEach(bb -> combined.put(bb));
    combined.rewind();
    return combined;
  }

  private void empty(Function<List<ByteBuffer>, String> errorResult, List<ByteBuffer> records) {
    String readable = errorResult.apply(records);
    assertEquals("Found records: " + readable, Lists.newArrayList(), records);
  }

  public void cleanup() throws Exception {
    this.manager.cleanup(status);
  }

  public static void verifyRecordMatchesJson(SchemaStore store, Map<String, Object> json,
    GenericRecord record) {
    LOG.debug("Comparing \nJSON: " + json + "\nRecord: " + record);
    SchemaUtil schema = new SchemaUtil(store, record);
    filterJson(json).forEach(entry -> {
      // search through each of the aliases to find a matching name in the record
      String aliasName = entry.getKey();
      String cname = schema.getCanonicalName(aliasName);
      // ensure the value matches
      assertNotNull("Didn't find a matching canonical name for " + aliasName, cname);
      assertEquals("JSON: " + json + "\nRecord: " + record,
        entry.getValue(), record.get(cname));
    });
    LOG.info("Record matches JSON!");
  }

  public static Stream<Map.Entry<String, Object>> filterJson(Map<String, Object> json) {
    return json.entrySet()
               .stream()
               .filter(entry -> AvroSchemaEncoder.IS_BASE_FIELD.negate().test(entry.getKey()));
  }

  public LambdaClientProperties getProps() {
    return this.props;
  }

  public ProgressTracker getProgress() {
    return this.progress;
  }

  public class ProgressTracker {
    private final SchemaStore store;
    private byte[] sent;
    private Map<String, Object> json;
    public GenericRecord avro;

    public ProgressTracker(SchemaStore store) {
      this.store = store;
    }

    public void sent(byte[] send) {
      this.sent = send;
    }

    public void sending(Map<String, Object> json) {
      LOG.info("Sending message: " + json);
      this.json = json;
    }

    public byte[] getSent() {
      return sent;
    }

    public Map<String, Object> getJson() {
      return json;
    }

    public GenericRecord getAvro() {
      return avro;
    }
  }
}
