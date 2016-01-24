package io.fineo.lambda.dynamo;

/**
 *
 */
public class ResultOrException<RESULT> {

  private Exception exception;
  private RESULT result;

  public void setResult(RESULT result){
    this.result = result;
  }

  public void setException(Exception e){
    this.exception = e;
  }

  public boolean hasException(){
    return this.exception != null;
  }

  public ResultOrException<RESULT> withException(Exception exception) {
    setException(exception);
    return this;
  }

  public Exception getException() {
    return exception;
  }
}
