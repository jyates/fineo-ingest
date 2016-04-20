package io.fineo.lambda.handle;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.Provider;
import io.fineo.lambda.FailureHandler;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A lambda handler that handles Kinesis events
 */
public abstract class KinesisHandler implements LambdaHandler<KinesisEvent> {

  private static final Log LOG = LogFactory.getLog(KinesisHandler.class);

  private final Provider<FirehoseBatchWriter> archive;
  private final Provider<FirehoseBatchWriter> processErrors;
  private final Provider<FirehoseBatchWriter> commitFailures;
  private boolean hasProcessingError;

  public KinesisHandler(Provider<FirehoseBatchWriter> archive,
    Provider<FirehoseBatchWriter> processErrors,
    Provider<FirehoseBatchWriter> commitFailures) {
    this.archive = archive;
    this.processErrors = processErrors;
    this.commitFailures = commitFailures;
  }

  @Override
  public void handle(KinesisEvent event) throws IOException {
    LOG.trace("Entering handler");
    int count = 0;
    for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
      count++;
      try {
        ByteBuffer data = rec.getKinesis().getData();
        data.mark();
        archive.get().addToBatch(data);
        data.reset();
        handleEvent(rec);
      } catch (RuntimeException e) {
        LOG.error("Failed for process record", e);
        addRecordError(rec);
      }
    }
    LOG.info("Handled " + count + " kinesis records");

    flushErrors();

    archive.get().flush();

    MultiWriteFailures<GenericRecord> failures = commit();
    LOG.debug("Finished writing record batches");
    FailureHandler.handle(failures, this.commitFailures::get);
  }

  protected abstract void handleEvent(KinesisEvent.KinesisEventRecord rec) throws IOException;

  protected abstract MultiWriteFailures<GenericRecord> commit() throws IOException;

  private void addRecordError(KinesisEvent.KinesisEventRecord rec) {
    ByteBuffer buff = rec.getKinesis().getData();
    buff.reset();
    processErrors.get().addToBatch(rec.getKinesis().getData());
    this.hasProcessingError = true;
  }

  private void flushErrors() throws IOException {
    if (!hasProcessingError) {
      LOG.debug("No error records found!");
      return;
    }

    LOG.trace("Flushing malformed records");
    processErrors.get().flush();
    LOG.debug("Flushed malformed records");
  }
}
