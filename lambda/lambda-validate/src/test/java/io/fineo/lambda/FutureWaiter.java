package io.fineo.lambda;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
class FutureWaiter {
  private final ListeningExecutorService executor;
  private List<ListenableFuture> futures = new ArrayList<>();

  public FutureWaiter(ListeningExecutorService executor){
    this.executor = executor;
  }

  public void run(Runnable r) {
    ListenableFuture future = executor.submit(r);
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
    latch.await();
  }
}
