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

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof FailedIngestFile))
      return false;

    FailedIngestFile that = (FailedIngestFile) o;

    if (!getOrg().equals(that.getOrg()))
      return false;
    if (!getFile().equals(that.getFile()))
      return false;
    return getMessage().equals(that.getMessage());

  }

  @Override
  public int hashCode() {
    int result = getOrg().hashCode();
    result = 31 * result + getFile().hashCode();
    result = 31 * result + getMessage().hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "FailedIngestFile{" +
           "org='" + org + '\'' +
           ", file='" + file + '\'' +
           ", message='" + message + '\'' +
           '}';
  }
}
