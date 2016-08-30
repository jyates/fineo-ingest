package io.fineo.lambda.e2e.aws.lambda;

import com.amazonaws.AbortedException;
import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.lambda.e2e.util.IngestUtil;
import io.fineo.lambda.e2e.manager.IKinesisStreams;
import io.fineo.lambda.util.run.FutureWaiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Connect lambda to a remote kinesis streams
 */
public class LocalLambdaRemoteKinesisConnector extends LambdaKinesisConnector<IngestUtil.Lambda> {

  private static final Logger LOG =
    LoggerFactory.getLogger(LocalLambdaRemoteKinesisConnector.class);
  private final BlockingDeque<Boolean> DONE_QUEUE = new LinkedBlockingDeque<>(1);
  private ExecutorService executor;
  protected boolean done;

  @Override
  public void connect(IKinesisStreams kinesisConnection) throws IOException {
    super.connect(kinesisConnection);
    connectStreams();
  }

  /**
   * Connect existing streams to the local lambda functions
   */
  protected void connectStreams() {
    this.executor = MoreExecutors.getExitingExecutorService(
      // Same as Executors#newSingleThreadExecutor, but we need a threadpoolexecutor, so copy/paste
      new ThreadPoolExecutor(1, 1,
        0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()),
      // addFirehose timeouts for the thread to done
      120, TimeUnit.SECONDS);

    // create each stream
    for (String stream : this.mapping.keySet()) {
      kinesis.setup(stream);
    }

    executor.execute(() -> {
      try {
        Map<String, BlockingQueue<List<ByteBuffer>>> streams = new HashMap<>();
        for (String stream : mapping.keySet()) {
          streams.put(stream, this.kinesis.getEventQueue(stream));
        }
        // keep trying to read records from the streams until we are told to stop
        while (!done) {
          tryProcessing(streams);
          Thread.sleep(50);
        }
      } catch (AbortedException e) {
        LOG.warn("Aborted while processing kinesis reads. This can happen when we stop the "
                 + "executor. Reason: " + e.getMessage());
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while processing!");
        if (done) {
          return;
        }
      } finally {
        LOG.info("Adding marker that CONNECT is done");
        DONE_QUEUE.add(true);
      }
    });
  }

  private void tryProcessing(Map<String, BlockingQueue<List<ByteBuffer>>> streams) {
    boolean more = true;
    while (!done && more) {
      more = false;
      // do a pass through the lambdas to see if there is any data to handleEvent
      for (Map.Entry<String, List<IngestUtil.Lambda>> stream : mapping.entrySet()) {
        String streamName = stream.getKey();
        LOG.debug("Reading from stream -> " + streamName);
        BlockingQueue<List<ByteBuffer>> queue = streams.get(streamName);
        // no more data for this stream, go onto the next on
        List<ByteBuffer> data = queue.poll();
        if (data == null) {
          continue;
        }

        // call the stream with the data
        for (IngestUtil.Lambda method : stream.getValue()) {
          LOG.info("--- Starting Method Call ---");
          Instant start = Instant.now();
          method.call(data);
          Duration done = Duration.between(start, Instant.now());
          LOG.info("---> Duration: " + done.toMillis() + " ms for " + method);
        }

        // this queue has more data to process, try again
        if (queue.peek() != null) {
          more = true;
        }
      }
    }
  }

  @Override
  public void cleanup(FutureWaiter futures) {
    LOG.info("Wait for CONNECT to done");
    this.done = true;
    this.executor.shutdown();
    this.executor.shutdownNow();
    try {
      DONE_QUEUE.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    LOG.info("DONE connecting");
    this.done = false;
  }
}
