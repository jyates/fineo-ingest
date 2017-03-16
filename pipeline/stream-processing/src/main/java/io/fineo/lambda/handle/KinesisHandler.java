package io.fineo.lambda.handle;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.Provider;
import io.fineo.internal.customer.Malformed;
import io.fineo.internal.customer.StackTraceElement;
import io.fineo.internal.customer.Thrown;
import io.fineo.lambda.FailureHandler;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

/**
 * A lambda handler that handles Kinesis events
 */
public abstract class KinesisHandler implements LambdaHandler<KinesisEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisHandler.class);
  public static final String FINEO_INTERNAL_ERROR_API_KEY = "_fineo";

  private final Provider<IFirehoseBatchWriter> archive;
  private final Provider<IFirehoseBatchWriter> processErrors;
  private final Provider<IFirehoseBatchWriter> commitFailures;
  private final Clock clock;
  private final AvroEventHandler errorHandler;
  private boolean hasProcessingError;

  public KinesisHandler(Provider<IFirehoseBatchWriter> archive,
    Provider<IFirehoseBatchWriter> processErrors,
    Provider<IFirehoseBatchWriter> commitFailures,
    Clock clock, AvroEventHandler handler) {
    this.archive = archive;
    this.processErrors = processErrors;
    this.commitFailures = commitFailures;
    this.clock = clock;
    this.errorHandler = handler;
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
      } catch (TenantBoundFineoException e) {
        addErrorRecord(rec, e.getApikey(), e.getWriteTime(), e);
      } catch (RuntimeException e) {
        LOG.error("Failed to process record", e);
        addErrorRecord(rec, e);
      }
    }
    LOG.info("Handled " + count + " kinesis records");

    flushErrors();

    archive.get().flush();

    MultiWriteFailures<GenericRecord, ?> failures = commit();
    LOG.debug("Finished writing record batches, checking if we need to write failures");
    FailureHandler.handle(failures, this.commitFailures::get);
  }

  /**
   * Handle an unexpected error that isn't tied to a particular user
   *
   * @param rec
   * @param e
   * @throws IOException
   */
  private void addErrorRecord(KinesisEvent.KinesisEventRecord rec, RuntimeException e)
    throws IOException {
    addErrorRecord(rec, null, -1, e);
  }

  private void addErrorRecord(KinesisEvent.KinesisEventRecord rec, String apiKey, long
    writeTime, Exception thrown)
    throws IOException {
    if (apiKey == null) {
      apiKey = FINEO_INTERNAL_ERROR_API_KEY;
    }
    if (writeTime < 0) {
      writeTime = clock.millis();
    }
    ByteBuffer data = rec.getKinesis().getData();
    data.reset();
    Malformed mal = new Malformed(apiKey, thrown.getMessage(), data, writeTime, toThrown(thrown));
    ByteBuffer buff = this.errorHandler.write(mal);
    buff.mark();
    processErrors.get().addToBatch(buff);
    this.hasProcessingError = true;
  }

  protected abstract void handleEvent(KinesisEvent.KinesisEventRecord rec) throws IOException;

  protected abstract MultiWriteFailures<GenericRecord, ?> commit() throws IOException;

  private void flushErrors() throws IOException {
    if (!hasProcessingError) {
      LOG.debug("No error records found!");
      return;
    }

    LOG.trace("Flushing malformed records");
    processErrors.get().flush();
    LOG.debug("Flushed malformed records");
  }

  public static List<Thrown> toThrown(Exception e) {
    List<Thrown> causes = new ArrayList<>();
    Throwable current = e;
    do {
      causes.add(toInnerThrown(current));
    } while ((current = current.getCause()) != null);

    // add the top as the first cause
    return causes;
  }

  public static Thrown toInnerThrown(Throwable e) {
    java.lang.StackTraceElement[] elems = e.getStackTrace();
    List<StackTraceElement> stack = new ArrayList<>(elems.length);
    for (java.lang.StackTraceElement elem : elems) {
      stack.add(StackTraceElement.newBuilder()
                                 .setDeclaringClass(elem.getClassName())
                                 .setFileName(elem.getFileName())
                                 .setLineNumber(elem.getLineNumber())
                                 .setMethodName(elem.getMethodName())
                                 .build());
    }
    return new Thrown(e.getMessage(), stack);
  }
}
