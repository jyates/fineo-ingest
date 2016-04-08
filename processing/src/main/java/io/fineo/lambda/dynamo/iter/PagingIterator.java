package io.fineo.lambda.dynamo.iter;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ForwardingBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private static final Log LOG = LogFactory.getLog(PagingIterator.class);

  private AtomicBoolean openRequest = new AtomicBoolean(false);
  // TODO support dynamic prefetch for better interaction with dynamodb's variable size paging
  private final int prefetch;
  private int count = 0;
  private Object poison = new Object();
  private final PageManager<T> supplier;
  private TrackedBlockingQueue items = new TrackedBlockingQueue(new LinkedBlockingQueue<>());
  private volatile boolean closed;

  public PagingIterator(int prefetchSize, PageManager<T> supplier) {
    this.prefetch = prefetchSize;
    this.supplier = supplier;
    this.supplier.prepare(this);
  }

  public Iterable<T> iterable() {
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
    // check to see that something was added to the queue
    if (items.wasAdded()) {
      items.setAdded(false);
    } else {
      makeRequest();
    }
  }

  private void makeRequest() {
    // make the request if we don't have any open request
    if (openRequest.compareAndSet(false, true)) {
      supplier.update(items, () -> completedBatch());
    }
  }

  private class TrackedBlockingQueue<E> extends ForwardingBlockingQueue<E> {

    private final BlockingQueue<E> delegate;
    private boolean added;

    public TrackedBlockingQueue(BlockingQueue<E> delegate) {
      this.delegate = delegate;
    }

    @Override
    protected BlockingQueue<E> delegate() {
      return delegate;
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
      this.added = true;
      return super.addAll(collection);
    }

    @Override
    public boolean add(E element) {
      this.added = true;
      return super.add(element);
    }

    public boolean wasAdded() {
      return added;
    }

    public void setAdded(boolean added) {
      this.added = added;
    }
  }
}
