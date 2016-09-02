package io.fineo.batch.processing.dynamo;

/**
 *
 */
public class FailedIngestFile {

  private final String org;
  private final String file;
  private final String message;

  public FailedIngestFile(String org, String file, String message) {
    this.org = org;
    this.file = file;
    this.message = message;
  }

  public String getOrg() {
    return org;
  }

  public String getFile() {
    return file;
  }

  public String getMessage() {
    return message;
  }
}
