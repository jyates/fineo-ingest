package io.fineo.lambda.configure;

public enum StreamType {
  ARCHIVE("archive"), PROCESSING_ERROR("error"), COMMIT_ERROR("error.commit");

  private final String suffix;

  StreamType(String suffix) {
    this.suffix = suffix;
  }

 public String getPropertyKey(String prefix) {
    return prefix + "." + suffix;
  }
}
