package io.fineo.lambda.util;

import io.fineo.internal.customer.Metric;
import io.fineo.lambda.LambdaAvroToStorage;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.LambdaRawRecordToAvro;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.AvroToDynamoWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.schema.avro.AvroRecordDecoder;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static io.fineo.lambda.LambdaClientProperties.StreamType;
import static io.fineo.lambda.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Helper utility to implement an end-to-end test of the lambda architecture
 */
public class EndToEndTestUtil {

  private static final Log LOG = LogFactory.getLog(EndToEndTestUtil.class);

  private final LambdaClientProperties props;

  private Map<String, List<ByteBuffer>> firehoseWrites = new HashMap<>();
  private List<GenericRecord> dynamoWrites = new ArrayList<>();
  private AvroToDynamoWriter dynamo;
  private Map<String, FirehoseBatchWriter> firehoses = new HashMap<>();
  private IngestUtil util;
  private SchemaStore store;

  public EndToEndTestUtil(Properties props) throws NoSuchMethodException {
    this.props = new LambdaClientProperties(props);
    setup();
  }

  private void setup() throws NoSuchMethodException {
    setupMocks();
    // setup each stage
    this.store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    LambdaRawRecordToAvro start = new LambdaRawRecordToAvro();
    start
      .setupForTesting(props, store, null,
        firehoses.get(props.getFirehoseStream(RAW_PREFIX, StreamType.ARCHIVE)),
        firehoses.get(props.getFirehoseStream(RAW_PREFIX, StreamType.PROCESSING_ERROR)),
        firehoses.get(props.getFirehoseStream(RAW_PREFIX, StreamType.COMMIT_ERROR)));

    LambdaAvroToStorage storage = new LambdaAvroToStorage();
    storage
      .setupForTesting(props, dynamo,
        firehoses.get(props.getFirehoseStream(STAGED_PREFIX, StreamType.ARCHIVE)),
        firehoses.get(props.getFirehoseStream(STAGED_PREFIX, StreamType.PROCESSING_ERROR)),
        firehoses.get(props.getFirehoseStream(STAGED_PREFIX, StreamType.COMMIT_ERROR)));

    // setup the flow
    this.util = IngestUtil.builder(store).start(start)
                          .then(props.getRawToStagedKinesisStreamName(), storage)
                          .build();
  }

  private void setupMocks() {
    this.dynamo = Mockito.mock(AvroToDynamoWriter.class);
    Mockito.doAnswer(invocationOnMock -> {
      dynamoWrites.add((GenericRecord) invocationOnMock.getArguments()[0]);
      return null;
    }).when(dynamo).write(Mockito.any(GenericRecord.class));
    Mockito.when(dynamo.flush()).thenReturn(new MultiWriteFailures(Collections.emptyList()));

    Stream.of(
      props.getFirehoseStream(RAW_PREFIX, StreamType.ARCHIVE),
      props.getFirehoseStream(RAW_PREFIX, StreamType.COMMIT_ERROR),
      props.getFirehoseStream(RAW_PREFIX, StreamType.PROCESSING_ERROR),
      props.getFirehoseStream(STAGED_PREFIX, StreamType.ARCHIVE),
      props.getFirehoseStream(STAGED_PREFIX, StreamType.COMMIT_ERROR),
      props.getFirehoseStream(STAGED_PREFIX, StreamType.PROCESSING_ERROR))
         .forEach(name -> {
      FirehoseBatchWriter firehose = Mockito.mock(FirehoseBatchWriter.class);
      firehoses.put(name, firehose);
      Mockito.doAnswer(invocation -> {
        get(firehoseWrites, name).add((ByteBuffer) invocation.getArguments()[0]);
        return null;
      }).when(firehose).addToBatch(Mockito.any());
    });
  }

  public void run(Map<String, Object> json) throws Exception {
    util.send(json);
  }

  public List<ByteBuffer> getFirehoseWrites(String firehoseName) {
    return firehoseWrites.get(firehoseName);
  }

  public List<GenericRecord> getDynamoWrites() {
    return dynamoWrites;
  }

  static <T> List<T> get(Map<String, List<T>> map, String key) {
    List<T> list = map.get(key);
    if (list == null) {
      list = new ArrayList<>();
      map.put(key, list);
    }
    return list;
  }

  public SchemaStore getStore() {
    return this.store;
  }

  public static void verifyRecordMatchesJson(SchemaStore store, Map<String, Object> json,
    GenericRecord record) {
    LOG.debug("Comparing \nJSON: " + json + "\nRecord: " + record);
    AvroRecordDecoder decoder = new AvroRecordDecoder(record);
    Metric metric = store.getMetricMetadata(decoder.getMetadata());
    Map<String, List<String>> names =
      metric.getMetadata().getCanonicalNamesToAliases();
    json.entrySet()
        .stream()
        .filter(entry -> AvroSchemaEncoder.IS_BASE_FIELD.negate().test(entry.getKey()))
        .forEach(entry -> {
          // search through each of the aliases to find a matching name in the record
          String aliasName = entry.getKey();
          String cname = null;
          for (Map.Entry<String, List<String>> nameToAliases : names.entrySet()) {
            if (nameToAliases.getValue().contains(aliasName)) {
              cname = nameToAliases.getKey();
              break;
            }
          }
          // ensure the value matches
          assertNotNull("Didn't find a matching canonical name for "+aliasName, cname);
          assertEquals("JSON: " + json + "\nRecord: " + record,
            entry.getValue(), record.get(cname));
        });
  }
}