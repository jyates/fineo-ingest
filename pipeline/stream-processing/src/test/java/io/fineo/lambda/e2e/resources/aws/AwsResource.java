package io.fineo.lambda.e2e.resources.aws;

import io.fineo.lambda.util.run.FutureWaiter;


public interface AwsResource {

 default void cleanup(FutureWaiter futures){}
}
