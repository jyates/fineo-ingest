package io.fineo.lambda.e2e.resources;

import io.fineo.lambda.util.run.FutureWaiter;

/**
 *
 */
public interface AwsResource {

  void cleanup(FutureWaiter futures);
}
