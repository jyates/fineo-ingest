package io.fineo.lambda.util;

import io.fineo.lambda.e2e.state.EndtoEndSuccessStatus;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

/**
 *
 */
public interface IResourceManager {
  void setup() throws Exception;

  byte[] send(Map<String, Object> json) throws Exception;

  void cleanup(EndtoEndSuccessStatus status) throws Exception;

  List<ByteBuffer> getFirehoseWrites(String streamName);

  BlockingQueue<List<ByteBuffer>> getKinesisWrites(String rawToStagedKinesisStreamName);

  SchemaStore getStore();

  void verifyDynamoWrites(Stream<RecordMetadata> metadata, List<Map<String, Object>> json);

  void cleanupDynamoClient();
}
