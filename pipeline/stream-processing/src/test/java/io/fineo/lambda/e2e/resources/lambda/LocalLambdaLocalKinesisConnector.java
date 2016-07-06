package io.fineo.lambda.e2e.resources.lambda;

import io.fineo.lambda.e2e.resources.aws.lambda.LocalLambdaRemoteKinesisConnector;
import io.fineo.lambda.e2e.resources.kinesis.MockKinesisStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Like the {@link LocalLambdaRemoteKinesisConnector}, but connects 'fake' kinesis queues to the
 * local lambda functions
 */
public class LocalLambdaLocalKinesisConnector extends LocalLambdaRemoteKinesisConnector {

  private static final Logger LOG = LoggerFactory.getLogger(LocalLambdaLocalKinesisConnector.class);
  private final BlockingDeque<Boolean> DONE_QUEUE = new LinkedBlockingDeque<>(1);

  @Override
  protected void connectStreams() {
    super.connectStreams();
    LOG.info("Adding marker that CONNECT is complete");
    DONE_QUEUE.add(true);
  }

  @Override
  public void reset() {
    ((MockKinesisStreams) this.kinesis).reset();
    this.cleanup(null);
    LOG.info("Wait for CONNECT to complete");
    try {
      DONE_QUEUE.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    this.done = false;
  }
}
