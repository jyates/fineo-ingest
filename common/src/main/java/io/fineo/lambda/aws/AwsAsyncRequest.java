package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;

import java.util.function.Consumer;

/**
 * Wrapper around a base record and the service request to 'submit' that record
 */
public class AwsAsyncRequest<BASE_FIELD, REQUEST extends AmazonWebServiceRequest> {

  private final Consumer<AwsAsyncRequest<BASE_FIELD, REQUEST>> success;
  private final Consumer<AwsAsyncRequest<BASE_FIELD, REQUEST>> failure;
  protected BASE_FIELD base;
  protected REQUEST request;
  private Object result;
  private Exception exception;

  public AwsAsyncRequest(BASE_FIELD base, REQUEST request) {
    this(base, request, a -> {
    }, a -> {
    });
  }

  public AwsAsyncRequest(BASE_FIELD base, REQUEST request,
    Consumer<AwsAsyncRequest<BASE_FIELD, REQUEST>> successHandler,
    Consumer<AwsAsyncRequest<BASE_FIELD, REQUEST>> failureHandler) {
    this.base = base;
    this.request = request;
    this.success = successHandler;
    this.failure = failureHandler;
  }

  public REQUEST getRequest() {
    return this.request;
  }

  public BASE_FIELD getBaseRecord() {
    return this.base;
  }

  protected void setRequest(REQUEST request) {
    this.request = request;
  }


  /**
   * Get notified of an exception while processing
   *
   * @param exception the error message
   * @return <tt>true</tt> if this operation should be retried
   */
  public boolean onError(Exception exception) {
    this.exception = exception;
    this.failure.accept(this);
    return true;
  }

  public <RESULT> void onSuccess(REQUEST request, RESULT result) {
    this.result = result;
    this.success.accept(this);
  }

  public <RESULT> RESULT getResult() {
    return (RESULT) this.result;
  }

  public Exception getException() {
    return exception;
  }

  @Override
  public String toString() {
    return "AwsAsyncRequest{" +
           "base=" + base +
           ", request=" + request +
           '}';
  }
}
