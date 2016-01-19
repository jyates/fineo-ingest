package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;

/**
 *
 */
@FunctionalInterface
public interface Submitter<T extends AmazonWebServiceRequest, R> {

  void submit(T type, AsyncHandler<T, R> handler);
}
