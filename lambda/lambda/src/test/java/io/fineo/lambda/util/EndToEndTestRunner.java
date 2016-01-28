package io.fineo.lambda.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.avro.TestRecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.fineo.lambda.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.StreamType.ARCHIVE;
import static io.fineo.lambda.LambdaClientProperties.StreamType.COMMIT_ERROR;
import static io.fineo.lambda.LambdaClientProperties.StreamType.PROCESSING_ERROR;
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
  private final ProgressTracker progress;
  private final EndtoEndSuccessStatus status;

  public EndToEndTestRunner(LambdaClientProperties props, ResourceManager manager)
    throws Exception {
    this.props = props;
    this.manager = manager;
    manager.setup(props);
    this.progress = new ProgressTracker(manager.getStore());
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
    SchemaTestUtils.addNewOrg(store, orgId, metricType, otherFields.toArray(new String[0]));
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
    // ensure that we didn't write any errors in the first stage
    verifyNoStageErrors(RAW_PREFIX, bbs -> new String(combine(bbs).array()));

    List<ByteBuffer> archived = manager.getFirhoseWrites(props.getFirehoseStreamName(RAW_PREFIX,
      ARCHIVE));
    ByteBuffer data = combine(archived);

    // ensure the bytes match from the archived/sent
    String expected = new String(progress.sent);
    String actual = new String(data.array());
    assertArrayEquals(
      "---- Raw data ->\n[" + expected + "]\n----Archive content -> \n[" + actual + "] don't match",
      progress.sent, data.array());

    verifyAvroRecordsFromStream(manager.getKinesisWrites(props.getRawToStagedKinesisStreamName()));
  }

  private void validateAvroToStorage() throws IOException {
    verifyNoStageErrors(STAGED_PREFIX, bbs -> {
      try {
        return SchemaUtil.toString(readRecords(combine(bbs)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    // archive should be exactly the avro formatted json record
    String stream = props.getFirehoseStreamName(STAGED_PREFIX, ARCHIVE);
    verifyAvroRecordsFromStream(manager.getFirhoseWrites(stream));
    status.firehoseStreamCorrect(stream);

    // verify that we wrote the right things to DynamoDB
    RecordMetadata metadata = RecordMetadata.get(progress.avro);
    manager.verifyDynamoWrites(metadata, progress.json);
  }

  private void verifyAvroRecordsFromStream(List<ByteBuffer> parsedBytes) throws IOException {
    // read the parsed avro records
    List<GenericRecord> parsedRecords = readRecords(combine(parsedBytes));
    assertEquals("Got unexpected number of records: " + parsedRecords, 1, parsedRecords.size());
    GenericRecord record = parsedRecords.get(0);

    // org/schema naming
    TestRecordMetadata.verifyRecordMetadataMatchesExpectedNaming(record);
    verifyRecordMatchesJson(progress.store, progress.json, record);
    progress.avro = record;
  }

  private void verifyNoStageErrors(String stage, Function<List<ByteBuffer>, String> errorResult) {
    verifyNoFirehoseWrites(errorResult,
      props.getFirehoseStreamName(stage, PROCESSING_ERROR),
      props.getFirehoseStreamName(stage, COMMIT_ERROR));
  }

  private void verifyNoFirehoseWrites(Function<List<ByteBuffer>, String> errorResult, String...
    streams) {
    for (String stream : streams) {
      empty(errorResult, manager.getFirhoseWrites(stream));
      status.firehoseStreamCorrect(stream);
    }
  }

  private List<GenericRecord> readRecords(ByteBuffer data) throws IOException {
    List<GenericRecord> records = new ArrayList<>();
    FirehoseRecordReader<GenericRecord> recordReader =
      FirehoseRecordReader.create(data);
    GenericRecord record = recordReader.next();
    if (record != null) {
      records.add(record);
    }
    return records;
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
  }

  public static Stream<Map.Entry<String, Object>> filterJson(Map<String, Object> json) {
    return json.entrySet()
               .stream()
               .filter(entry -> AvroSchemaEncoder.IS_BASE_FIELD.negate().test(entry.getKey()));
  }

  private class ProgressTracker {
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
      this.json = json;
    }
  }
}