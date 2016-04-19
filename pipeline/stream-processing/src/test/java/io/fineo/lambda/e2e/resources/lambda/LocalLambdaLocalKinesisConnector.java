package io.fineo.lambda.e2e.resources.lambda;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
import io.fineo.lambda.e2e.resources.kinesis.MockKinesisStreams;

import java.io.IOException;
import java.util.Map;

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
