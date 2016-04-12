package io.fineo.lambda.util;

import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.e2e.EndtoEndSuccessStatus;
import io.fineo.schema.avro.RecordMetadata;
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

  default void cleanup(EndtoEndSuccessStatus status) throws Exception{};

  List<ByteBuffer> getFirehoseWrites(String streamName);

  List<ByteBuffer> getKinesisWrites(String rawToStagedKinesisStreamName);

  SchemaStore getStore();

  void verifyDynamoWrites(RecordMetadata metadata, Map<String, Object> json);

  default void reset(){}
}
