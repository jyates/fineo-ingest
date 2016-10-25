package io.fineo.lambda.handle;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.Provider;
import io.fineo.lambda.FailureHandler;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A lambda handler that handles Kinesis events
 */
public abstract class KinesisHandler implements LambdaHandler<KinesisEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisHandler.class);

  private final Provider<IFirehoseBatchWriter> archive;
  private final Provider<IFirehoseBatchWriter> processErrors;
  private final Provider<IFirehoseBatchWriter> commitFailures;
  private boolean hasProcessingError;

  public KinesisHandler(Provider<IFirehoseBatchWriter> archive,
    Provider<IFirehoseBatchWriter> processErrors,
    Provider<IFirehoseBatchWriter> commitFailures) {
    this.archive = archive;
    this.processErrors = processErrors;
    this.commitFailures = commitFailures;
  }

  @Override
  public void handle(KinesisEvent event) throws IOException {
    LOG.trace("Entering handler - {} records", event.getRecords().size());
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
        LOG.error("Failed to process record", e);
        addRecordError(rec);
      }
    }
    LOG.info("Handled " + count + " kinesis records");

    flushErrors();

    archive.get().flush();

    MultiWriteFailures<GenericRecord, ?> failures = commit();
    LOG.debug("Finished writing record batches, checking if we need to write failures");
    FailureHandler.handle(failures, this.commitFailures::get);
  }

  protected abstract void handleEvent(KinesisEvent.KinesisEventRecord rec) throws IOException;

  protected abstract MultiWriteFailures<GenericRecord, ?> commit() throws IOException;

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
