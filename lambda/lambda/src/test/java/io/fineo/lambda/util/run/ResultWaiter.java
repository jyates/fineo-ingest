package io.fineo.lambda.util.run;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.time.Duration;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Wait for results. Create a waiter with default timeouts/interval through a
 * {@link io.fineo.lambda.util.run.ResultWaiter.ResultWaiterFactory}
 */
public class ResultWaiter<RESULT> {
  private static final Log LOG = LogFactory.getLog(ResultWaiter.class);
  private long timeoutMs;
  private long intervalMs;
  private String description;
  private Supplier<RESULT> status;
  private Predicate<RESULT> statusCheck;
  private RESULT lastStatus;

  public static class ResultWaiterFactory {
    private final long timeout;
    private final long interval;

    public ResultWaiterFactory(Duration timeout, Duration interval) {
      this(timeout.toMillis(), interval.toMillis());
    }

    public ResultWaiterFactory(long timeout, long interval) {
      this.timeout = timeout;
      this.interval = interval;
    }

    public ResultWaiter get() {
      return new ResultWaiter().withInterval(interval).withTimeout(timeout);
    }
  }

  private ResultWaiter() {
  }

  /**
   * Do the action and return the result or return null if an exception is thrown
   */
  public static <T> T doOrNull(ThrowingSupplier<T> t) {
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
    Preconditions.checkArgument(intervalMs > 0, "Interval must be over 0ms");
    Preconditions.checkArgument(intervalMs <= timeoutMs,
      "Interval [%d] must be <= timeoutMs [%d]", intervalMs, timeoutMs);

    LOG.info("Waiting for [" + description + "]. Max wait: " + timeoutMs / 1000 + "s. Interval: "
             + this.intervalMs / 1000 + "s");
    long startTime = System.currentTimeMillis();
    long endTime = startTime + timeoutMs;
    int i = 0;
    // run at least once, but not longer than the specified endtime
    do {
      try {
        lastStatus = status.get();
        if (statusCheck.test(lastStatus)) {
          LOG.info("Finished waiting for: " + description + ". Elapsed: " +
                   ((System.currentTimeMillis() - startTime) / 1000.0) + "s");
          return true;
        }
      } catch (Exception e) {
        LOG.warn("[" + description + "] Got exception, but ignoring it...", e);
        // ignore
      }
      if ((i % 10) == 0 && i > 0) {
        LOG.info("(" + (i * intervalMs / 1000) + "s) Waiting [" + description + "]");
      }
      i++;
      Thread.sleep(intervalMs);
    } while (System.currentTimeMillis() < endTime);
    LOG.warn(String.format("Request [%s] didn't not become complete within %d sec!",
      description, timeoutMs / 1000));
    return false;
  }

  private void validate() {
    Preconditions.checkArgument(status != null, "Must have a status supplier");
    Preconditions.checkArgument(statusCheck != null, "Must have some status check");
  }


  public RESULT getLastStatus() {
    return lastStatus;
  }
}
