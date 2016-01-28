package io.fineo.lambda.util.mock;

import io.fineo.lambda.LambdaAvroToStorage;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.LambdaRawRecordToAvro;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.RecordMetadata;
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
import java.util.stream.Stream;

import static io.fineo.lambda.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.util.EndToEndTestRunner.verifyRecordMatchesJson;
import static org.junit.Assert.assertEquals;

/**
 * Manage a set of mock resources for the lambda workflow
 */
public class MockResourceManager implements ResourceManager {

  private Map<String, List<ByteBuffer>> firehoseWrites = new HashMap<>();
  private List<GenericRecord> dynamoWrites = new ArrayList<>();
  private AvroToDynamoWriter dynamo;
  private Map<String, FirehoseBatchWriter> firehoses = new HashMap<>();
  private SchemaStore store;
  private IngestUtil util;

  @Override
  public void setup(LambdaClientProperties props) throws NoSuchMethodException {
    setupMocks(props);
    // setup each stage
    this.store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    LambdaRawRecordToAvro start = new LambdaRawRecordToAvro();
    start
      .setupForTesting(props, store, null,
        firehoses
          .get(props.getFirehoseStreamName(RAW_PREFIX, LambdaClientProperties.StreamType.ARCHIVE)),
        firehoses.get(
          props.getFirehoseStreamName(RAW_PREFIX,
            LambdaClientProperties.StreamType.PROCESSING_ERROR)),
        firehoses.get(
          props.getFirehoseStreamName(RAW_PREFIX, LambdaClientProperties.StreamType.COMMIT_ERROR)));

    LambdaAvroToStorage storage = new LambdaAvroToStorage();
    storage
      .setupForTesting(props, dynamo,
        firehoses
          .get(props.getFirehoseStreamName(STAGED_PREFIX, LambdaClientProperties.StreamType.ARCHIVE)),
        firehoses.get(props
          .getFirehoseStreamName(STAGED_PREFIX, LambdaClientProperties.StreamType.PROCESSING_ERROR)),
        firehoses.get(
          props.getFirehoseStreamName(STAGED_PREFIX, LambdaClientProperties.StreamType.COMMIT_ERROR)));

    // setup the flow
    this.util = IngestUtil.newBuilder()
                          .start(start)
                          .then(props.getRawToStagedKinesisStreamName(), storage)
                          .build();
  }

  @Override
  public byte[] send(Map<String, Object> json) throws Exception {
    return util.send(json);
  }

  @Override
  public List<ByteBuffer> getFirhoseWrites(String streamName) {
    List<ByteBuffer> writes = this.firehoseWrites.get(streamName);
    return writes != null ? writes : new ArrayList<>(0);
  }

  @Override
  public List<ByteBuffer> getKinesisWrites(String streamName) {
    return this.util.getKinesisStream(streamName);
  }

  @Override
  public SchemaStore getStore() {
    return this.store;
  }

  @Override
  public void verifyDynamoWrites(RecordMetadata metadata, Map<String, Object> json) {
    assertEquals(1, dynamoWrites.size());
    verifyRecordMatchesJson(store, json, dynamoWrites.get(0));
  }

  private void setupMocks(LambdaClientProperties props) {
    this.dynamo = Mockito.mock(AvroToDynamoWriter.class);
    Mockito.doAnswer(invocationOnMock -> {
      dynamoWrites.add((GenericRecord) invocationOnMock.getArguments()[0]);
      return null;
    }).when(dynamo).write(Mockito.any(GenericRecord.class));
    Mockito.when(dynamo.flush()).thenReturn(new MultiWriteFailures(Collections.emptyList()));

    Stream.of(
      props.getFirehoseStreamName(RAW_PREFIX, LambdaClientProperties.StreamType.ARCHIVE),
      props.getFirehoseStreamName(RAW_PREFIX, LambdaClientProperties.StreamType.COMMIT_ERROR),
      props.getFirehoseStreamName(RAW_PREFIX, LambdaClientProperties.StreamType.PROCESSING_ERROR),
      props.getFirehoseStreamName(STAGED_PREFIX, LambdaClientProperties.StreamType.ARCHIVE),
      props.getFirehoseStreamName(STAGED_PREFIX, LambdaClientProperties.StreamType.COMMIT_ERROR),
      props.getFirehoseStreamName(STAGED_PREFIX, LambdaClientProperties.StreamType.PROCESSING_ERROR))
          .forEach(name -> {
            FirehoseBatchWriter firehose = Mockito.mock(FirehoseBatchWriter.class);
            firehoses.put(name, firehose);
            Mockito.doAnswer(invocation -> {
              ByteBuffer buff = (ByteBuffer) invocation.getArguments()[0];
              IngestUtil.get(firehoseWrites, name).add(buff.duplicate());
              return null;
            }).when(firehose).addToBatch(Mockito.any());
          });
  }

}
