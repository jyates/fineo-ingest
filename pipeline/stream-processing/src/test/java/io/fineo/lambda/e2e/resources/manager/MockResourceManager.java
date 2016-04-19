package io.fineo.lambda.e2e.resources.manager;

import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.EndtoEndSuccessStatus;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.fineo.lambda.configure.legacy.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.configure.legacy.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.e2e.EndToEndTestRunner.verifyRecordMatchesJson;
import static org.junit.Assert.assertEquals;

/**
 * Manage a set of mock resources for the lambda workflow
 */
public class MockResourceManager extends BaseResourceManager {

  private final Map<String, List<ByteBuffer>> firehoseWrites = new HashMap<>();
  private final List<GenericRecord> dynamoWrites = new ArrayList<>();
  private final Map<String, FirehoseBatchWriter> firehoses = new HashMap<>();
  private final IKinesisStreams streams;

  private AvroToDynamoWriter dynamo;
  private SchemaStore store;
  private LambdaClientProperties props;

  public MockResourceManager(LambdaKinesisConnector connector, SchemaStore store, IKinesisStreams streams) {
    super(connector);
    this.store = store;
    this.streams = streams;
  }

  @Override
  public void setup(LambdaClientProperties props) throws NoSuchMethodException, IOException {
    this.props = props;
    setupMocks(props);

    // connector manages kinesis itself in local mock
    this.connector.connect(streams);
  }

  public FirehoseBatchWriter getWriter(String prefix, StreamType type) {
    return firehoses.get(props.getFirehoseStreamName(prefix, type));
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
            .withStatusNull(() -> {
              List<ByteBuffer> writes = this.firehoseWrites.get(streamName);
              return writes != null && writes.size() > 0 ? writes : null;
            }).withDoneWhenNotNull();
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
      props.getFirehoseStreamName(RAW_PREFIX, StreamType.ARCHIVE),
      props.getFirehoseStreamName(RAW_PREFIX, StreamType.COMMIT_ERROR),
      props.getFirehoseStreamName(RAW_PREFIX, StreamType.PROCESSING_ERROR),
      props.getFirehoseStreamName(STAGED_PREFIX, StreamType.ARCHIVE),
      props.getFirehoseStreamName(STAGED_PREFIX, StreamType.COMMIT_ERROR),
      props
        .getFirehoseStreamName(STAGED_PREFIX, StreamType.PROCESSING_ERROR))
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

  public void cleanup(EndtoEndSuccessStatus status) throws Exception {
    for (List<ByteBuffer> firehose : firehoseWrites.values()) {
      firehose.clear();
    }

    this.dynamoWrites.clear();
    this.connector.reset();
  }

  public AvroToDynamoWriter getDynamo() {
    return this.dynamo;
  }

  public IKinesisStreams getStreams() {
    return streams;
  }
}
