package io.fineo.lambda.e2e.resources.aws.lambda;

import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Connect lambda to a remote kinesis streams
 */
public class LocalLambdaRemoteKinesisConnector extends LambdaKinesisConnector<IngestUtil.Lambda> {

  private static final Logger LOG =
    LoggerFactory.getLogger(LocalLambdaRemoteKinesisConnector.class);
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
      // add timeouts for the thread to complete
      120, TimeUnit.SECONDS);

    // create each stream
    for (String stream : this.mapping.keySet()) {
      kinesis.setup(stream);
    }

    executor.execute(() -> {
      Map<String, BlockingQueue<List<ByteBuffer>>> streams = new HashMap<>();
      for (String stream : mapping.keySet()) {
        streams.put(stream, this.kinesis.getEventQueue(stream));
      }

      while (!done) {
        for (Map.Entry<String, List<IngestUtil.Lambda>> stream : mapping.entrySet()) {
          String streamName = stream.getKey();
          LOG.debug("Reading from stream -> " + streamName);
          BlockingQueue<List<ByteBuffer>> queue = streams.get(streamName);
          List<ByteBuffer> data = queue.poll();
          // while there is more data to read from the queue, read it
          while (data != null && !done) {
            for (IngestUtil.Lambda method : stream.getValue()) {
              LOG.info("--- Starting Method Call ---");
              Instant start = Instant.now();
              method.call(data);
              Duration done = Duration.between(start, Instant.now());
              LOG.info("---> Duration: " + done.toMillis() + " ms for " + method);
            }
            data = queue.poll();
          }
        }
      }
      LOG.info("Done connecting to local streams!");
    });
  }

  @Override
  public void cleanup(FutureWaiter futures) {
    this.done = true;
    this.executor.shutdown();
    this.executor.shutdownNow();
  }
}
