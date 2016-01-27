package io.fineo.lambda.dynamo.iter;

import java.util.Queue;
import java.util.function.BiConsumer;

/**
 * Helper to make paging batches easier. Handles notifying the {@link PagingIterator} when the
 * batch completes and when the there are no more values from batching iterator
 */
@FunctionalInterface
public interface BatchedPager<T> extends BiConsumer<Queue<T>, PagingIterator<T>> {

  default void accept(Queue<T> queue, PagingIterator<T> iter) {
    if (page(queue)) {
      iter.done();
    } else {
      iter.completedBatch();
    }
  }

  boolean page(Queue<T> queue);
}