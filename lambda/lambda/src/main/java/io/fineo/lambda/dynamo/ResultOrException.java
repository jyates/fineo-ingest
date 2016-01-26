package io.fineo.lambda.dynamo;

import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class ResultOrException<RESULT> {

  private CountDownLatch setLatch = new CountDownLatch(1);
  private Exception exception;
  private RESULT result;

  public void setResult(RESULT result){
    this.result = result;
    setLatch.countDown();
  }

  public void setException(Exception e){
    this.exception = e;
    setLatch.countDown();
  }

  public boolean hasException(){
    return this.exception != null;
  }

  public Exception getException() {
    return exception;
  }

  public RESULT getResult() {
    return this.result;
  }

  public void await() throws InterruptedException {
    this.setLatch.await();
  }
}
