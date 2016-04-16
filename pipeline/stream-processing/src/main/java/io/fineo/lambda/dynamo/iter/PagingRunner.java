package io.fineo.lambda.dynamo.iter;

import java.util.Queue;

/**
 *
 */
public interface PagingRunner<T> {
  /**
   * Get the next page of results
   *
   * @param queue to receive the results
   * @param doneNotifier
   * @param batchComplete
   */
  void page(Queue queue, VoidCallWithArg<PagingRunner<T>> doneNotifier,
    Runnable batchComplete);
}
