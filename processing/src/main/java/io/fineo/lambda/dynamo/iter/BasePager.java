package io.fineo.lambda.dynamo.iter;

/**
 *
 */
public abstract class BasePager<T> implements PagingRunner<T> {

  private VoidCallWithArg<PagingRunner> done;

  public void page(Pipe<T> queue, VoidCallWithArg<PagingRunner> doneNotifier) {
    this.done = doneNotifier;
    page(queue);
  }

  protected abstract void page(Pipe<T> queue);

  protected void complete() {
    this.done.call(this);
  }
}
