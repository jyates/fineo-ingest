package io.fineo.lambda.e2e.resources.lambda;

import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
import io.fineo.lambda.e2e.resources.kinesis.MockKinesisStreams;

import java.io.IOException;
import java.util.Map;

/**
 * Like the {@link LocalLambdaRemoteKinesisConnector}, but connects 'fake' kinesis queues to the
 * local lambda functions
 */
public class LocalLambdaLocalKinesisConnector extends LocalLambdaRemoteKinesisConnector {

  public LocalLambdaLocalKinesisConnector(Map streamToLambdaMapping, String pipelineSource) {
    super(streamToLambdaMapping, pipelineSource);
  }

  @Override
  public void connect(LambdaClientProperties props, IKinesisStreams ignored) throws IOException {
    this.kinesis = new MockKinesisStreams();
    connectStreams();
  }

  @Override
  public void reset() {
    ((MockKinesisStreams)this.kinesis).reset();
  }
}
