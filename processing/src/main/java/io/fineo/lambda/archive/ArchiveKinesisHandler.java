package io.fineo.lambda.archive;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Archive records from a Kinesis event to firehose
 */
public class ArchiveKinesisHandler {
  private static final Log LOG = LogFactory.getLog(ArchiveKinesisHandler.class);
  private Function<String, FirehoseBatchWriter> writer;

  public ArchiveKinesisHandler(Function<String, FirehoseBatchWriter> writer) {
    this.writer = writer;
  }

  public void handle(KinesisEvent event) throws IOException {
    LOG.trace("Entering handler");
    FirehoseBatchWriter archive = null;
    for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
      if (archive == null) {
        String[] topics = rec.getEventSourceARN().split(":");
        archive = writer.apply(topics[topics.length - 1]);
      }
      archive.addToBatch(rec.getKinesis().getData());
    }

    if (archive != null) {
      archive.flush();
    }
  }
}
