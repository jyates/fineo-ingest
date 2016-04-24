package io.fineo.lambda.dynamo.iter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Queue;

/**
 *
 */
public abstract class BasePager<T> implements PagingRunner<T> {

  private static final Log LOG = LogFactory.getLog(BasePager.class);
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

  protected void complete() {
    this.done.call(this);
    batchComplete();
  }

  protected void batchComplete() {
    this.batchComplete.run();
  }
}
