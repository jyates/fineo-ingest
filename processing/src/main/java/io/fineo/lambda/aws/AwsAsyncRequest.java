package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;

/**
 * Wrapper around a base record and the service request to 'submit' that record
 */
public class AwsAsyncRequest<B, R extends AmazonWebServiceRequest> {

  protected B base;
  private R request;

  public AwsAsyncRequest(B base, R request) {
    this.base = base;
    this.request = request;
  }

  public R getRequest(){
    return this.request;
  }

  public B getBaseRecord(){
    return this.base;
  }

  @Override
  public String toString() {
    return "AwsAsyncRequest{" +
           "base=" + base +
           ", request=" + request +
           '}';
  }
}
