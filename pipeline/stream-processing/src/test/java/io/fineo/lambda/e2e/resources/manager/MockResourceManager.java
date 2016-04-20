package io.fineo.lambda.e2e.resources.manager;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.EndtoEndSuccessStatus;
import io.fineo.lambda.e2e.resources.dynamo.MockAvroToDynamo;
import io.fineo.lambda.e2e.resources.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.firehose.LocalFirehoseStreams;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Manage a set of mock resources for the lambda workflow
 */
public class MockResourceManager extends BaseResourceManager {

  private final IKinesisStreams streams;
  private final LocalFirehoseStreams firehoses;

  private MockAvroToDynamo dynamo;
  private SchemaStore store;

  public MockResourceManager(LambdaKinesisConnector connector, SchemaStore store,
    IKinesisStreams streams) {
    super(connector);
    this.store = store;
    this.streams = streams;
    this.firehoses = new LocalFirehoseStreams();
  }

  @Override
  public void setup(LambdaClientProperties props) throws NoSuchMethodException, IOException {
    setupMocks(props);

    // connector manages kinesis itself in local mock
    this.connector.connect(streams);
  }

  public FirehoseBatchWriter getWriter(String prefix, StreamType type) {
    return firehoses.getWriter(prefix, type);
  }

  @Override
  public List<ByteBuffer> getFirehoseWrites(String streamName) {
    return firehoses.getFirehoseWrites(streamName);
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
   this.dynamo.verifyWrites(metadata, json);
  }

  private void setupMocks(LambdaClientProperties props) {
   this.dynamo = new MockAvroToDynamo(store);
    firehoses.setup(props);
  }

  @Override
  public void cleanup(EndtoEndSuccessStatus status) throws Exception {
    this.firehoses.cleanup();
    this.dynamo.cleanup();
    this.connector.reset();
  }

  public AvroToDynamoWriter getDynamo() {
    return this.dynamo.getWriter();
  }

  public IKinesisStreams getStreams() {
    return streams;
  }
}
