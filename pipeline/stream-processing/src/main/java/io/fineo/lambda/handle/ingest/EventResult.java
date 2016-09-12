package io.fineo.lambda.handle.ingest;

public class EventResult {
  private String errorCode;
  private String errorMessage;

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }
}
