package io.fineo.lambda.dynamo;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Iterator that supports paging results in via the supplied {@link Function}. The supplier can
 * asynchronously add elements to the queue when it is called.
 * <p>
 * Periodically, as specified in the constructor, <tt>this</tt> will attempt to prefetch the next
 * 'batch' of results after the number of elapsed calls to {@link #next()}.  The prefetch
 * executes asynchronously if the supplied {@link Function} also completes asynchronously.
 * </p>
 * <p>
 * When the function is done creating more results, it should call {@link #done()} to prevent
 * <tt>this</tt> from making any more prefetch attempts and prevents any new record from being
 * read in the queue.
 * </p>
 * <p>
 * This is thread-safe.
 * </p>
 */
public class PagingIterator<T> implements Iterator<T> {

  private final int prefetch;
  private int count = 0;
  private Object poison = new Object();
  private final BiFunction<Queue<T>, PagingIterator<T>, Void> supplier;
  private BlockingQueue items = new LinkedBlockingQueue<>();
  private Object next;
  private volatile boolean closed;

  public PagingIterator(int prefetchSize, BiFunction<Queue<T>, PagingIterator<T>, Void> supplier) {
    this.prefetch = prefetchSize;
    this.supplier = supplier;
    supplier.apply(items, this);
  }

  @Override
  public boolean hasNext() {
    if (next == null) {
      getNext();
      if (next == poison) {
        return false;
      }
    }
    return next != poison;
  }

  private void getNext() {
    try {
      next = items.take();
    } catch (InterruptedException e) {
      if (this.closed) {
        return;
      }
      Thread.currentThread().interrupt();
      getNext();
    }
    return;
  }

  @Override
  public T next() {
    // first time
    if (next == null) {
      getNext();
    }
    // got to the poison pill
    if (next == poison) {
      throw new NoSuchElementException("No more elements!");
    }
    T o = (T) next;
    next = null;
    if (count++ > prefetch || !closed) {
      supplier.apply(items, this);
      count = 0;
    }
    return o;
  }

  public void done() {
    this.closed = true;
    this.items.add(poison);
  }
}