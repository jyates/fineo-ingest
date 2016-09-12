package io.fineo.lambda.handle.ingest;

public class EventResult {
  private String errorCode;
  private String errorMessage;

  public String getErrorCode() {
    return errorCode;
  }

  public EventResult setErrorCode(String errorCode) {
    this.errorCode = errorCode;
    return this;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public EventResult setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
    return this;
  }
}
