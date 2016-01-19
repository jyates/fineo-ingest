package io.fineo.lambda;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.test.TestableLambda;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 *
 */
public abstract class BaseLambda implements TestableLambda {

  private static final Log LOG = LogFactory.getLog(BaseLambda.class);
  protected LambdaClientProperties props;
  private FirehoseBatchWriter errors;

  public void handler(KinesisEvent event) throws IOException {
    setupInternal();
    handleEventInternal(event);
    LOG.info("Finished!");
  }

  @Override
  public void handleEventInternal(KinesisEvent event) throws IOException {
    LOG.trace("Entering handler");
    for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
      try {
        handleEvent(rec);
      } catch (RuntimeException e) {
        addRecordError(rec);
      }
    }

    flushErrors();

    MultiWriteFailures<GenericRecord> failures = commit();
    LOG.debug("Finished writing record batches");
    FailureHandler.handle(failures, getFailedEventHandler());
  }

  private void addRecordError(KinesisEvent.KinesisEventRecord rec) {
    if (this.errors == null) {
      this.errors = getErrorEventHandler().get();
    }
    ByteBuffer buff = rec.getKinesis().getData();
    buff.reset();
    errors.addToBatch(rec.getKinesis().getData());
  }

  private void flushErrors() throws IOException {
    if (errors == null) {
      LOG.debug("No error records found!");
      return;
    }
    LOG.trace("Flushing malformed records");
    errors.flush();
    LOG.debug("Flushed malformed records");
  }

  abstract void handleEvent(KinesisEvent.KinesisEventRecord rec) throws IOException;

  abstract MultiWriteFailures<GenericRecord> commit() throws IOException;

  private void setupInternal() throws IOException {
    LOG.debug("Setting up");
    this.props = LambdaClientProperties.load();

    setup();
  }

  protected void setup() throws IOException {
    // noop
  }

  abstract Supplier<FirehoseBatchWriter> getFailedEventHandler();

  abstract Supplier<FirehoseBatchWriter> getErrorEventHandler();
}
