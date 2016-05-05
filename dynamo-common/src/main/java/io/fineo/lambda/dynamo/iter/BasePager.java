package io.fineo.lambda.dynamo.iter;

import java.util.Queue;

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

  /**
   * Call this when you have no more results (pages) left. Either this method or {@link
   * #batchComplete()} should be called, but not both. If both are called, ensure that <b>this
   * method is called first</b>.
   */
  protected void complete() {
    this.done.call(this);
    batchComplete();
  }

  /**
   * Call this when the current batch of lookups (page) is complete. Either this method or {@link
   * #complete()} should be called, but not both.
   */
  protected void batchComplete() {
    this.batchComplete.run();
  }
}
