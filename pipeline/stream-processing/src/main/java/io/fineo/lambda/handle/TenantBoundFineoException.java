package io.fineo.lambda.handle;

/**
 * A runtime exception for a failure that is tied back to a specified tenant id (apikey)
 */
public class TenantBoundFineoException extends RuntimeException{
  private final String apikey;
  private final long writeTime;

  public TenantBoundFineoException(String message, Throwable cause, String apikey) {
    this(message, cause, apikey, -1);
  }

  public TenantBoundFineoException(String message, Throwable cause, String apikey, long writeTime) {
    super(message, cause);
    this.apikey = apikey;
    this.writeTime = writeTime;
  }

  public String getApikey() {
    return apikey;
  }

  public long getWriteTime() {
    return writeTime;
  }
}
