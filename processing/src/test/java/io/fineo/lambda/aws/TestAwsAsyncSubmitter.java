package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.datapipeline.model.ActivatePipelineRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * This class is supposed to be thread-safe, so we need to some complex ordering testing around
 * completing tasks and flushing.
 */
public class TestAwsAsyncSubmitter {

  private static final Log LOG = LogFactory.getLog(TestAwsAsyncSubmitter.class);

  /**
   * Bug test to ensure that we use the phaser correctly to handle when elements are completing.
   * A phaser enters 'termination' state when we have no more outstanding items, but that can
   * occur if we have a slow set of incoming requests and a fast first request, which terminates
   * the phaser before we can even submit all the requests
   *
   * @throws Exception
   */
  @Test
  public void testTaskCompletesBeforeNextSubmissionOrFlush() throws Exception {
    int tasks = 2;
    Executor exec = Executors.newFixedThreadPool(tasks);
    long seed = System.currentTimeMillis();
    LOG.info("Using seed: " + seed);
    Random rand = new Random(seed);
    CountDownLatch done = new CountDownLatch(1);
    Submitter<ActivatePipelineRequest, String> client = (request, handler) -> exec.execute(() -> {
      boolean first = request.getPipelineId().equals("0");
      try {
        long wait = first ? 0 : rand.nextInt(500);
        LOG.info("Sleeping for: " + wait);
        Thread.currentThread().sleep(wait);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      done.countDown();
      handler.onSuccess(request, "done");
    });
    AwsAsyncSubmitter<ActivatePipelineRequest, String, Integer> submitter = new
      AwsAsyncSubmitter<>(1, client);


    for (int i = 0; i < tasks; i++) {
      ActivatePipelineRequest request = new ActivatePipelineRequest().withPipelineId(Integer
        .toString(i));
      if (i > 0) {
        done.await();
        Thread.sleep(1000);
      }
      submitter.submit(new AwsAsyncRequest<>(i, request));

    }
    submitter.flush();
  }

  @Test(timeout = 10000)
  public void testSubmitDuringFlush() throws Exception {
    int tasks = 2;
    Executor exec = Executors.newFixedThreadPool(tasks + 1);
    Submitter<CountDownLatchRequest, String> client = (request, handler) -> exec.execute(() -> {
      try {
        request.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      request.done();
      handler.onSuccess(request, "done");
    });
    AwsAsyncSubmitter<CountDownLatchRequest, String, Integer> submitter = new
      AwsAsyncSubmitter<>(1, client);

    // submit a task that blocks until we are done
    CountDownLatchRequest task1 = new CountDownLatchRequest();
    submitter.submit(new AwsAsyncRequest<>(1, task1));
    CountDownLatch flushing = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      flushing.countDown();
      submitter.flush();
    }
    );
    t.start();

    // wait for flushing to start + some buffer
    flushing.await();
    Thread.sleep(500);

    // submit a new request
    CountDownLatchRequest task2 = new CountDownLatchRequest();
    submitter.submit(new AwsAsyncRequest<>(2, task2));

    task2.started.await();
    task1.start.countDown();

    // wait for the flush thread to complete.
    t.join();
    assertEquals("Task two was able to start", 1, task2.start.getCount());
  }

  private class CountDownLatchRequest extends AmazonWebServiceRequest {

    private CountDownLatch start = new CountDownLatch(1);
    private CountDownLatch started = new CountDownLatch(1);
    private CountDownLatch done = new CountDownLatch(1);

    public void await() throws InterruptedException {
      started.countDown();
      start.await();
    }

    public void done() {
      done.countDown();
    }
  }
}
