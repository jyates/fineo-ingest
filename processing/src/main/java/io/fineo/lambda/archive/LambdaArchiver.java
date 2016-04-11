package io.fineo.lambda.archive;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.common.base.Functions;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.firehose.FirehoseBatchWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Actual thing that gets called by AWS for processing. However, it is just a lightweight dummy
 * for creating the actual thing that processes the message
 */
public class LambdaArchiver {

  private static Function<String, FirehoseBatchWriter> FUNC =
    new Function<String, FirehoseBatchWriter>() {
      public LambdaClientProperties props;

      @Override
      public FirehoseBatchWriter apply(String phase) {
        if (this.props == null) {
          try {
            props = LambdaClientProperties.load();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        String name = props.getFirehoseStreamName(phase, LambdaClientProperties.StreamType.ARCHIVE);
        return new FirehoseBatchWriter(props,
          (Function<ByteBuffer, ByteBuffer>) Functions.identity(),
          name);
      }
    };

  private ArchiveSnsHandler snsHandler;
  private ArchiveKinesisHandler kinesisHandler;

  public void handle(SNSEvent event) throws Exception {
    if (this.snsHandler == null) {
      this.snsHandler = new ArchiveSnsHandler(FUNC);
    }
    this.snsHandler.handle(event);
  }

  public void handle(KinesisEvent event) throws Exception{
    if (this.kinesisHandler == null) {
      this.kinesisHandler = new ArchiveKinesisHandler(FUNC);
    }
    this.kinesisHandler.handle(event);
  }
}
