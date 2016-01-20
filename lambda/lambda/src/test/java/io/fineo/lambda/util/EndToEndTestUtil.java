package io.fineo.lambda.util;

import io.fineo.lambda.LambdaAvroToStorage;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.LambdaRawRecordToAvro;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.AvroToDynamoWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
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

/**
 * Helper utility to implement an end-to-end test of the lambda architecture
 */
public class EndToEndTestUtil {

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
        firehoses.get(props.getFirehoseRawArchiveStreamName()),
        firehoses.get(props.getFirehoseRawProcessErrorStreamName()),
        firehoses.get(props.getFirehoseRawCommitFailureStreamName()));

    LambdaAvroToStorage storage = new LambdaAvroToStorage();
    storage
      .setupForTesting(props, dynamo,
        firehoses.get(props.getFirehoseStagedArchiveStreamName()),
        firehoses.get(props.getFirehoseStagedDyanmoErrorStreamName()),
        firehoses.get(props.getFirehoseStagedFailedCommitStreamName()));

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
      props.getFirehoseRawArchiveStreamName(),
      props.getFirehoseRawCommitFailureStreamName(),
      props.getFirehoseRawProcessErrorStreamName(),
      props.getFirehoseStagedArchiveStreamName(),
      props.getFirehoseStagedDyanmoErrorStreamName(),
      props.getFirehoseStagedFailedCommitStreamName())
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
}