package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class FlushResponse<BASE_REQUEST, REQUEST extends AmazonWebServiceRequest> {
  private final List<AwsAsyncRequest<BASE_REQUEST, REQUEST>> completed;
  private final MultiWriteFailures<BASE_REQUEST, REQUEST> failures;

  public FlushResponse(List<AwsAsyncRequest<BASE_REQUEST, REQUEST>> completed,
    MultiWriteFailures failures) {
    this.completed = completed;
    this.failures = failures;
  }

  public MultiWriteFailures<BASE_REQUEST, REQUEST> getFailures() {
    return failures;
  }

  public List<AwsAsyncRequest<BASE_REQUEST, REQUEST>> getCompleted() {
    return completed;
  }
}
