package io.fineo.lambda.dynamo.iter;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.fineo.lambda.dynamo.ResultOrException;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.UnaryOperator;

/**
 *
 */
public interface PagingRunner<T> {
  /**
   * Get the next page of results
   *
   * @param queue to receive the results
   */
  void page(Pipe<T> queue, VoidCallWithArg<PagingRunner> doneNotifier);
}
