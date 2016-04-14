package io.fineo.etl.processing;

import io.fineo.lambda.FailureHandler;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public abstract class BaseProcessor<INFO> {

  private static final Log LOG = LogFactory.getLog(BaseProcessor.class);
  private final Supplier<FirehoseBatchWriter> processingErrors;
  private final Supplier<FirehoseBatchWriter> commitErrors;
  private final OutputWriter<Message<INFO>> writer;

  public BaseProcessor(Supplier<FirehoseBatchWriter> processingErrors,
    Supplier<FirehoseBatchWriter> commitErrors, OutputWriter<Message<INFO>> writer) {
    this.writer = writer;
    this.processingErrors = processingErrors;
    this.commitErrors = commitErrors;
  }

  public void handle(String json) throws IOException {
    try {
      process(json, this.writer);
    } catch (RuntimeException | IOException e) {
      LOG.error("Failure during record processing!", e);
      processingErrors.get().addToBatch(ByteBuffer.wrap(json.getBytes()));
    }

    flushErrors();

    MultiWriteFailures<GenericRecord> failures = this.writer.commit();
    LOG.debug("Finished writing record batches");
    FailureHandler.handle(failures, commitErrors);
  }

  private void flushErrors() throws IOException {
    processingErrors.get().flush();
  }

  protected abstract void process(String json, OutputWriter<Message<INFO>> writer) throws IOException;
}
