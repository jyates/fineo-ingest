package io.fineo.lambda.archive;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.common.base.Functions;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.firehose.FirehoseBatchWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Actual thing that gets called by AWS for processing. However, it is just a lightweight dummy
 * for creating the actual thing that processes the message
 */
public class LambdaSqsArchiver {

  private static Function<String, FirehoseBatchWriter> FUNC = phase -> {
    LambdaClientProperties props = null;
    try {
      props = LambdaClientProperties.load();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String name = props.getFirehoseStreamName(phase, LambdaClientProperties.StreamType.ARCHIVE);
    return new FirehoseBatchWriter(props, (Function<ByteBuffer, ByteBuffer>) Functions.identity(),
      name);
  };

  public void handle(SNSEvent event) throws Exception {
    new ArchiveHandler(FUNC).handle(event);
  }
}
