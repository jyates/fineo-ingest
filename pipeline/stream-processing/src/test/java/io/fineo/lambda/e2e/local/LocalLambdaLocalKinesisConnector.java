package io.fineo.lambda.e2e.local;

import io.fineo.lambda.e2e.aws.lambda.LocalLambdaRemoteKinesisConnector;
import io.fineo.lambda.util.run.FutureWaiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Like the {@link LocalLambdaRemoteKinesisConnector}, but connects 'fake' kinesis queues to the
 * local lambda functions
 */
public class LocalLambdaLocalKinesisConnector extends LocalLambdaRemoteKinesisConnector {

  private static final Logger LOG = LoggerFactory.getLogger(LocalLambdaLocalKinesisConnector.class);

  @Override
  public void cleanup(FutureWaiter futures) {
    super.cleanup(futures);
    ((MockKinesisStreams) this.kinesis).reset();
  }
}
