package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class ResultOrException<RESULT> {

  private CountDownLatch setLatch = new CountDownLatch(1);
  private Exception exception;
  private RESULT result;

  public ResultOrException(Exception exception) {
    setException(exception);
  }

  public ResultOrException(RESULT r) {
    setResult(r);
  }

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

  public void doThrow() {
    if(!hasException()){
      return;
    }
    if(exception instanceof RuntimeException){
      throw (RuntimeException)exception;
    }
    throw new RuntimeException(exception);
  }

  @Override
  public String toString() {
    return "ResultOrException{" +
           "exception=" + exception +
           ", result=" + result +
           '}';
  }
}
