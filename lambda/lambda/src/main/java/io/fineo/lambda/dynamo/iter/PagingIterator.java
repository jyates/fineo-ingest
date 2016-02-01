package io.fineo.lambda.dynamo.iter;

import com.google.common.collect.AbstractIterator;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
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
public class PagingIterator<T> extends AbstractIterator<T> {

  private AtomicBoolean openRequest = new AtomicBoolean(false);
  // TODO support dynamic prefetch for better interaction with dynamodb's variable size paging
  private final int prefetch;
  private int count = 0;
  private Object poison = new Object();
  private final BiConsumer<Queue<T>, PagingIterator<T>> supplier;
  private BlockingQueue items = new LinkedBlockingQueue<>();
  private volatile boolean closed;

  public PagingIterator(int prefetchSize, BiConsumer<Queue<T>, PagingIterator<T>> supplier) {
    this.prefetch = prefetchSize;
    this.supplier = supplier;
  }

  public Iterable<T> iterable(){
    return () -> this;
  }

  @Override
  protected T computeNext() {
    Object next = getNext();

    // do the prefetch, if we aren't closed
    if (++count >= prefetch && !closed) {
      makeRequest();
      count = 0;
    }

    // got to the poison pill, so we are done
    if (next == poison || next == null) {
      endOfData();
      return null;
    }
    return (T) next;
  }

  private Object getNext() {
    try {
      // light test to see if there is data in the queue. If not, attempt to request more data,
      // which will open a new request for more data, if there isn't one already
      Object next = items.poll(1, TimeUnit.MILLISECONDS);
      if (next == null) {
        makeRequest();
        next = items.take();
      }
      return next;
    } catch (InterruptedException e) {
      if (this.closed) {
        return null;
      }
      Thread.currentThread().interrupt();
      return getNext();
    }
  }

  public void done() {
    // this doesn't close the request because we don't want any new requests started after we close
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.items.add(poison);
  }

  public void completedBatch() {
    boolean closed = openRequest.compareAndSet(true, false);
    assert closed : "No open request to close!";
  }

  private void makeRequest() {
    // make the request if we don't have any open request
    if (openRequest.compareAndSet(false, true)) {
      supplier.accept(items, this);
    }
  }
}