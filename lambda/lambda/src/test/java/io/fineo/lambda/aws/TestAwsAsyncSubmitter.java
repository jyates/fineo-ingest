package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 *
 */
public class TestAwsAsyncSubmitter {

  private static final Log LOG = LogFactory.getLog(TestAwsAsyncSubmitter.class);
  @Test
  public void testRandomRunTimes() throws Exception{
    int tasks = 5;
    Executor exec = Executors.newFixedThreadPool(tasks);
    long seed = System.currentTimeMillis();
    LOG.info("Using seed: "+seed);
    Random rand = new Random(seed);
    Submitter<AmazonWebServiceRequest, String> client = (type, handler) -> exec.execute(() ->{
      try {
        long wait = rand.nextInt(5000);
        LOG.info("Sleeping for: "+wait);
        Thread.currentThread().sleep(wait);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      handler.onSuccess(type, "done");
    });
    AwsAsyncSubmitter<AmazonWebServiceRequest, String, Integer> submitter = new
      AwsAsyncSubmitter<>(1, client);


    for (int i = 0; i < tasks; i++) {
      submitter.submit(new AwsAsyncRequest<>(i, null));
    }
    submitter.flush();
  }
}
