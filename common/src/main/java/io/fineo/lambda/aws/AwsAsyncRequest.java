package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;

/**
 * Wrapper around a base record and the service request to 'submit' that record
 */
public class AwsAsyncRequest<BASE_FIELD, REQUEST extends AmazonWebServiceRequest> {

  protected BASE_FIELD base;
  protected REQUEST request;

  public AwsAsyncRequest(BASE_FIELD base, REQUEST request) {
    this.base = base;
    this.request = request;
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

  @Override
  public String toString() {
    return "AwsAsyncRequest{" +
           "base=" + base +
           ", request=" + request +
           '}';
  }

  /**
   * Get notified of an exception while processing
   * @param exception the error message
   * @return <tt>true</tt> if this operation should be retried
   */
  public boolean onError(Exception exception) {
    return true;
  }

  public <RESULT> void onSuccess(REQUEST request, RESULT result) {
    // noop
  }
}
