package io.fineo.lambda.e2e.resources.manager;

import io.fineo.lambda.LambdaAvroToStorage;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.LambdaRawRecordToAvro;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.lambda.LocalLambdaKinesisConnector;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.fineo.lambda.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.e2e.EndToEndTestRunner.verifyRecordMatchesJson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Manage a set of mock resources for the lambda workflow
 */
public class MockResourceManager extends BaseResourceManager {

  private final Map<String, List<ByteBuffer>> firehoseWrites = new HashMap<>();
  private final List<GenericRecord> dynamoWrites = new ArrayList<>();
  private final Map<String, FirehoseBatchWriter> firehoses = new HashMap<>();

  private final LambdaRawRecordToAvro start;
  private final LambdaAvroToStorage storage;

  private AvroToDynamoWriter dynamo;
  private SchemaStore store;

  public MockResourceManager(LocalLambdaKinesisConnector connector, LambdaRawRecordToAvro start,
    LambdaAvroToStorage storage) {
    super(connector);
    this.start = start;
    this.storage = storage;
  }

  @Override
  public void setup(LambdaClientProperties props) throws NoSuchMethodException, IOException {
    setupMocks(props);
    // setup each stage
    this.store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    start
      .setupForTesting(props, store, null,
        firehoses
          .get(props.getFirehoseStreamName(RAW_PREFIX, LambdaClientProperties.StreamType.ARCHIVE)),
        firehoses.get(
          props.getFirehoseStreamName(RAW_PREFIX,
            LambdaClientProperties.StreamType.PROCESSING_ERROR)),
        firehoses.get(
          props.getFirehoseStreamName(RAW_PREFIX, LambdaClientProperties.StreamType.COMMIT_ERROR)));
    storage
      .setupForTesting(props, dynamo,
        firehoses
          .get(
            props.getFirehoseStreamName(STAGED_PREFIX, LambdaClientProperties.StreamType.ARCHIVE)),
        firehoses.get(props
          .getFirehoseStreamName(STAGED_PREFIX,
            LambdaClientProperties.StreamType.PROCESSING_ERROR)),
        firehoses.get(
          props
            .getFirehoseStreamName(STAGED_PREFIX, LambdaClientProperties.StreamType.COMMIT_ERROR)));

    this.connector.connect(props);
  }

  @Override
  public List<ByteBuffer> getFirehoseWrites(String streamName) {
    if (streamName == null) {
      return new ArrayList<>(0);
    }
    ResultWaiter.ResultWaiterFactory waiter = new ResultWaiter.ResultWaiterFactory(
      TestProperties.ONE_MINUTE, TestProperties.ONE_SECOND);
    ResultWaiter<List<ByteBuffer>> wait =
      waiter.get()
            .withDescription("Waiting for firehose writes to propagate...")
            .withStatus(() -> this.firehoseWrites.get(streamName))
            .withStatusCheck(a -> a != null);
    wait.waitForResult();
    return wait.getLastStatus();
  }

  @Override
  public List<ByteBuffer> getKinesisWrites(String streamName) {
    return this.connector.getWrites(streamName);
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
      props
        .getFirehoseStreamName(STAGED_PREFIX, LambdaClientProperties.StreamType.PROCESSING_ERROR))
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
