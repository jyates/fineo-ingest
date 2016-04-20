package io.fineo.lambda.e2e.resources.lambda;

import io.fineo.lambda.e2e.resources.aws.lambda.LocalLambdaRemoteKinesisConnector;
import io.fineo.lambda.e2e.resources.kinesis.MockKinesisStreams;

/**
 * Like the {@link LocalLambdaRemoteKinesisConnector}, but connects 'fake' kinesis queues to the
 * local lambda functions
 */
public class LocalLambdaLocalKinesisConnector extends LocalLambdaRemoteKinesisConnector {

  @Override
  public void reset() {
    ((MockKinesisStreams)this.kinesis).reset();
  }
}
