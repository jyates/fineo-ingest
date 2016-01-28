package io.fineo.lambda;

import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 *
 */
class ResultWaiter<RESULT> {
  private static final Log LOG = LogFactory.getLog(ResultWaiter.class);
  private long timeoutMs = AwsResourceManager.THREE_HUNDRED_SECONDS;
  private long intervalMs = AwsResourceManager.ONE_SECOND;
  private String description;
  private Supplier<RESULT> status;
  private Predicate<RESULT> statusCheck;

  /**
   * Do the action and return the result or return null if an exception is thrown
   *
   * @param t
   * @param <T>
   * @return
   */
  static <T> T doOrNull(ThrowingSupplier<T> t) {
    try {
      return t.a();
    } catch (Exception e) {
      return null;
    }
  }

  public ResultWaiter withTimeout(long timeout) {
    this.timeoutMs = timeout;
    return this;
  }

  public ResultWaiter withInterval(long interval) {
    this.intervalMs = interval;
    return this;
  }

  public ResultWaiter withDescription(String description) {
    this.description = description;
    return this;
  }

  public ResultWaiter withStatus(Supplier<RESULT> status) {
    this.status = status;
    return this;
  }

  public ResultWaiter withStatusNull(ThrowingSupplier<RESULT> status) {
    this.status = () -> doOrNull(status);
    return this.withNullStatusCheck();
  }

  public ResultWaiter withStatusCheck(Predicate<RESULT> statusCheck) {
    this.statusCheck = statusCheck;
    return this;
  }

  public ResultWaiter withNullStatusCheck() {
    this.statusCheck = a -> a == null;
    return this;
  }

  public boolean waitForResult() {
    validate();
    try {
      return run();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean run() throws InterruptedException {
    if (intervalMs > 0 && intervalMs < timeoutMs) {
      LOG.info(
        "Waiting for [" + description + "]. Max wait: " + timeoutMs / 1000 + "s");
      long startTime = System.currentTimeMillis();
      long endTime = startTime + (long) timeoutMs;
      for (; System.currentTimeMillis() < endTime; Thread.sleep((long) intervalMs)) {
        try {
          if (statusCheck.test(status.get())) {
            return true;
          }
        } catch (ResourceNotFoundException var11) {
        }
      }
      LOG.warn(String.format("Resource [%s] didn't not become active/created within %d sec!",
        description, timeoutMs / 1000));
      return false;
    } else {
      throw new IllegalArgumentException("Interval must be > 0 and < timeoutMs");
    }
  }

  private void validate() {
    Preconditions.checkArgument(status != null, "Must have a status supplier");
    Preconditions.checkArgument(statusCheck != null, "Must have some status check");
  }
}
