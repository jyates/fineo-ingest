package io.fineo.lambda.e2e.util;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A Blocking queue that wraps a backing list. Allows clients to poll the list.
 */
public class WrappingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

  private static final int WAIT_INTERVAL = 100;
  private final List<T> backingList;
  private final int offset;
  private int index = 0;

  public WrappingQueue(List<T> byteBuffers, int offset) {
    this.backingList = byteBuffers;
    this.offset = offset;
  }

  @Override
  public Iterator<T> iterator() {
    // iterator that skips past the previously existing elements
    Iterator<T> delegate = backingList.iterator();
    for (int i = 0; i < offset; i++) {
      delegate.next();
    }
    return delegate;
  }

  @Override
  public T take() throws InterruptedException {
    T next = poll();
    while (next == null) {
      Thread.currentThread().sleep(WAIT_INTERVAL);
      next = poll();
    }
    return next;
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    long end = System.currentTimeMillis() + unit.toMillis(timeout);
    T next = poll();
    while (next == null) {
      long now = System.currentTimeMillis();
      if (now > end || now + WAIT_INTERVAL > end) {
        return null;
      }

      Thread.currentThread().sleep(WAIT_INTERVAL);
      next = poll();
    }
    return next;
  }

  @Override
  public T poll() {
    T next = peek();
    if (next != null) {
      index++;
    }
    return next;
  }

  @Override
  public T peek() {
    if (backingList.size() <= (index + offset)) {
      return null;
    }
    try {
      return backingList.get(index);
    } catch (IndexOutOfBoundsException e) {
      // for debugging hooks, when things go sideways
      throw e;
    }
  }


  @Override
  public int size() {
    return backingList.size() - offset - index;
  }

  @Override
  public void put(T t) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int drainTo(Collection<? super T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(T t) {
    throw new UnsupportedOperationException();
  }
}
