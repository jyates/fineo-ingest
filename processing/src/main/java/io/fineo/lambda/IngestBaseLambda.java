package io.fineo.lambda;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.test.TestableLambda;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.fineo.lambda.LambdaClientProperties.StreamType;

/**
 * Base class for AWS Lambda functions that do ingest. Handles things like iterating events,
 * archiving the raw bytes (for replay later) and error handling.
 */
public abstract class IngestBaseLambda implements TestableLambda {

  private static final Log LOG = LogFactory.getLog(IngestBaseLambda.class);
  protected String phaseName;
  protected LambdaClientProperties props;
  private FirehoseBatchWriter archive;
  private FirehoseBatchWriter processErrors;
  private FirehoseBatchWriter commitFailures;

  protected IngestBaseLambda(String phase) {
    this.phaseName = phase;
  }

  public void handler(KinesisEvent event) throws IOException {
    setupInternal();
    handleEventInternal(event);
  }

  @Override
  public void handleEventInternal(KinesisEvent event) throws IOException {
    LOG.trace("Entering handler");
    for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
      try {
        ByteBuffer data = rec.getKinesis().getData();
        data.mark();
        archive.addToBatch(data);
        data.reset();
        handleEvent(rec);
      } catch (RuntimeException | IOException e) {
        addRecordError(rec);
      }
    }

    flushErrors();

    archive.flush();

//    MultiWriteFailures<GenericRecord> failures = commit();
//    LOG.debug("Finished writing record batches");
//    FailureHandler.handle(failures,
//      failures != null ? () -> this.commitFailures : getCommitFailureStream());
  }

  private void addRecordError(KinesisEvent.KinesisEventRecord rec) {
    if (this.processErrors == null) {
      this.processErrors = getProcessingErrorStream().get();
    }
    ByteBuffer buff = rec.getKinesis().getData();
    buff.reset();
    processErrors.addToBatch(rec.getKinesis().getData());
  }

  private void flushErrors() throws IOException {
    if (processErrors == null) {
      LOG.debug("No error records found!");
      return;
    }
    LOG.trace("Flushing malformed records");
    processErrors.flush();
    LOG.debug("Flushed malformed records");
  }

  abstract void handleEvent(KinesisEvent.KinesisEventRecord rec) throws IOException;

  abstract MultiWriteFailures<GenericRecord> commit() throws IOException;

  private void setupInternal() throws IOException {
    LOG.debug("Setting up");
    this.props = props != null ? props : LambdaClientProperties.load();

    setup();

    this.archive =
      new FirehoseBatchWriter(props, ByteBuffer::duplicate, getArchiveStreamName());
  }

  protected void setup() throws IOException {
    // noop
  }

  @VisibleForTesting
  public void setupForTesting(FirehoseBatchWriter archive, FirehoseBatchWriter processErrors,
    FirehoseBatchWriter commitFailures) {
    this.archive = archive;
    this.processErrors = processErrors;
    this.commitFailures = commitFailures;
  }

  private String getArchiveStreamName() {
    return props.getFirehoseStreamName(phaseName, StreamType.ARCHIVE);
  }

  protected Supplier<FirehoseBatchWriter> getCommitFailureStream() {
    return lazyFirehoseBatchWriter(props.getFirehoseStreamName(phaseName, StreamType.COMMIT_ERROR));
  }

  protected Supplier<FirehoseBatchWriter> getProcessingErrorStream() {
    return lazyFirehoseBatchWriter(props.getFirehoseStreamName(phaseName, StreamType.PROCESSING_ERROR));
  }

  protected Supplier<FirehoseBatchWriter> lazyFirehoseBatchWriter(String stream) {
    return curriedFirehose.apply(stream).apply(ByteBuffer::duplicate);
  }

  protected Supplier<FirehoseBatchWriter> lazyFirehoseBatchWriter(String stream,
    Function<ByteBuffer, ByteBuffer> transform) {
    return curriedFirehose.apply(stream).apply(transform);
  }

  private Function<String, Function<Function<ByteBuffer, ByteBuffer>,
    Supplier<FirehoseBatchWriter>>>
    curriedFirehose =
    name -> func -> () -> new FirehoseBatchWriter(props, func, name);


  @VisibleForTesting
  public static void setupPropertiesForIntegrationTesting(IngestBaseLambda lambda,
    LambdaClientProperties props) {
    lambda.props = props;
  }
}
