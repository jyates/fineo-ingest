package io.fineo.lambda.dynamo.iter;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.fineo.lambda.dynamo.ResultOrException;

import java.util.Map;

/**
 *
 */
public interface PagingRunner<T> {
  // TODO consider making an abstract base class that handles setting complete and ignoring any
  // more data sent to the Pipe in page()
  boolean complete();

  /**
   * Get the next page of results
   *
   * @param queue to receive the results
   */
  void page(Pipe<T> queue);
}
