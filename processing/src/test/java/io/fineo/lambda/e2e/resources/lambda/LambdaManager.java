package io.fineo.lambda.e2e.resources.lambda;

import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.e2e.resources.AwsResource;
import io.fineo.lambda.util.run.FutureWaiter;

/**
 * Manages lambda functions
 */
public class LambdaManager implements AwsResource {

  public void create(LambdaClientProperties props){
  }

  @Override
  public void cleanup(FutureWaiter futures) {
  }
}
