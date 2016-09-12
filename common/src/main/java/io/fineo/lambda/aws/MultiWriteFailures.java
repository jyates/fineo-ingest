package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;

import java.util.List;

/**
 * Any failures that have accumulated since the last time that 'flush' was called
 */
public class MultiWriteFailures<T, R extends AmazonWebServiceRequest> {

  private final List<AwsAsyncRequest<T, R>> actions;

  public MultiWriteFailures(List<AwsAsyncRequest<T, R>> failed) {
    this.actions = failed;
  }

  /**
   * Check to see if all the results are successful. If not, use
   *
   * @return
   */
  public boolean any() {
    return actions.size() > 0;
  }

  public List<AwsAsyncRequest<T, R>> getActions() {
    return actions;
  }
}
