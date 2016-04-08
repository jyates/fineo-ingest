package io.fineo.lambda.dynamo.iter;

import java.util.Queue;

/**
 *
 */
public abstract class BasePager<T> implements PagingRunner<T> {

  private VoidCallWithArg<PagingRunner<T>> done;
  private Runnable batchComplete;

  @Override
  public void page(Queue queue, VoidCallWithArg<PagingRunner<T>> doneNotifier,
    Runnable batchComplete) {
    this.done = doneNotifier;
    this.batchComplete = batchComplete;
    page(queue);
  }

  protected abstract void page(Queue<T> queue);

  protected final void complete() {
    this.done.call(this);
    batchComplete();
  }

  protected final void batchComplete() {
    this.batchComplete.run();
  }
}
