package io.fineo.etl.processing;

import io.fineo.lambda.FailureHandler;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
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
      Message<INFO> obj = process(json);
      this.writer.write(obj);
    } catch (RuntimeException | IOException e) {
      processingErrors.get().addToBatch(ByteBuffer.wrap(json.getBytes()));
    }

    flushErrors();

    MultiWriteFailures<Message> failures = this.writer.commit();
    LOG.debug("Finished writing record batches");
    FailureHandler.handle(failures, commitErrors);
  }

  private void flushErrors() throws IOException {
    processingErrors.get().flush();
  }

  protected abstract Message<INFO> process(String json) throws IOException;
}
