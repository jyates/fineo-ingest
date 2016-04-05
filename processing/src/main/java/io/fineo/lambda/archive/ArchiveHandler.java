package io.fineo.lambda.archive;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Archive records from an event to firehose
 */
public class ArchiveHandler {
  private static final Log LOG = LogFactory.getLog(ArchiveHandler.class);
  private Function<String, FirehoseBatchWriter> writer;

  public ArchiveHandler(Function<String, FirehoseBatchWriter> writer) {
    this.writer = writer;
  }

  public void handle(SNSEvent event) throws IOException {
    LOG.trace("Entering handler");
    FirehoseBatchWriter archive = null;
    for (SNSEvent.SNSRecord rec : event.getRecords()) {
      if (archive == null) {
        String[] topics = rec.getSNS().getTopicArn().split(":");
        archive = writer.apply(topics[topics.length - 1]);
      }
      archive.addToBatch(ByteBuffer.wrap(rec.getSNS().getMessage().getBytes("UTF-8")));
    }

    if (archive != null) {
      archive.flush();
    }
  }
}
