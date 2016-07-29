package io.fineo.lambda.util.run;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class FutureWaiter {
  private static final Log LOG = LogFactory.getLog(FutureWaiter.class);

  private final ListeningExecutorService executor;
  private List<ListenableFuture> futures = new ArrayList<>();
  private AtomicReference<Exception> exception = new AtomicReference<>();

  public FutureWaiter(ListeningExecutorService executor) {
    this.executor = executor;
  }

  public void run(Runnable r) {
    ListenableFuture future = executor.submit(() -> {
      try {
        r.run();
      } catch (Exception e) {
        setException(e);
      }
    });
    this.futures.add(future);
  }

  public void await() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(futures.size());
    for (ListenableFuture f : futures) {
      Futures.addCallback(f, new FutureCallback() {
        @Override
        public void onSuccess(Object result) {
          latch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
          latch.countDown();
        }
      });
    }
    try {
      latch.await();
      if (exception.get() != null) {
        throw new RuntimeException(exception.get());
      }
    } finally {
      this.futures.clear();
      this.exception.set(null);
    }
  }

  public void setException(Exception e) {
    LOG.error("Failed to complete task!", e);
    this.exception.set(e);
  }
}
