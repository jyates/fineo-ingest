package io.fineo.lambda.util;

import io.fineo.lambda.LambdaClientProperties;
import io.fineo.schema.store.SchemaStore;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface ResourceManager {
  void setup(LambdaClientProperties props) throws Exception;

  byte[] send(Map<String, Object> json) throws Exception;

  default void cleanup() throws Exception{};

  List<ByteBuffer> getFirhoseWrites(String streamName);

  List<ByteBuffer> getKinesisWrites(String rawToStagedKinesisStreamName);

  SchemaStore getStore();

  void verifyDynamoWrites(Map<String, Object> json);
}
