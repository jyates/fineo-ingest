package io.fineo.lambda.configure.legacy;

public enum StreamType {
  ARCHIVE("archive"), PROCESSING_ERROR("error"), COMMIT_ERROR("error.commit");

  private final String suffix;

  StreamType(String suffix) {
    this.suffix = suffix;
  }

  String getPropertyKey(String prefix) {
    return prefix + "." + suffix;
  }
}
