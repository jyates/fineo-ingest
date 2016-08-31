package io.fineo.lambda.firehose;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public interface IFirehoseBatchWriter {
  void addToBatch(ByteBuffer record);

  void flush() throws IOException;
}
