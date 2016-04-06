package io.fineo.lambda.e2e.resources.lambda;

import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.StreamProducer;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
import io.fineo.lambda.util.run.FutureWaiter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Connect lambda to a remote kinesis streams
 */
public class LocalLambdaRemoteKinesisConnector extends LambdaKinesisConnector<IngestUtil.Lambda> {

  protected IKinesisStreams kinesis;
  private ExecutorService executor = Executors.newSingleThreadExecutor();
  private boolean done;

  public LocalLambdaRemoteKinesisConnector(
    Map<String, List<IngestUtil.Lambda>> streamToLambdaMapping,
    String pipelineSource) {
    super(streamToLambdaMapping, pipelineSource);
  }

  @Override
  public void write(byte[] data) {
    write(source, data);
  }

  @Override
  public void write(String kinesisStream, byte[] data) {
    this.kinesis.submit(kinesisStream, ByteBuffer.wrap(data));
  }

  @Override
  public void connect(LambdaClientProperties props, IKinesisStreams kinesisConnection)
    throws IOException {
    this.kinesis = kinesisConnection;
    connectStreams();
  }

  /**
   * Connect existing streams to the local lambda functions
   */
  protected void connectStreams() {
    // create each stream
    for (String stream : this.mapping.keySet()) {
      kinesis.setup(stream);
    }

    // ensure that the outputs can actually write back to kinesis
    for (List<IngestUtil.Lambda> lambdas : this.mapping.values()) {
      for (IngestUtil.Lambda lambda : lambdas) {
        Object func = lambda.getFunction();
        if (func instanceof StreamProducer) {
          ((StreamProducer) func).setDownstreamForTesting(this.kinesis.getProducer());
        }
      }
    }

    executor.execute(() -> {
      Map<String, BlockingQueue<List<ByteBuffer>>> streams = new HashMap<>();
      for (String stream : mapping.keySet()) {
        streams.put(stream, this.kinesis.getEventQueue(stream));
      }

      while (!done) {
        for (Map.Entry<String, List<IngestUtil.Lambda>> stream : mapping.entrySet()) {
          BlockingQueue<List<ByteBuffer>> queue = streams.get(stream.getKey());
          List<ByteBuffer> data = queue.poll();
          // while there is more data to read from the queue, read it
          while (data != null) {
            for (IngestUtil.Lambda method : stream.getValue()) {
              method.call(data);
            }
            data = queue.poll();
          }
        }
      }
    });
  }

  @Override
  public List<ByteBuffer> getWrites(String streamName) {
    List<ByteBuffer> data = new ArrayList<>();
    for (List<ByteBuffer> buffs : this.kinesis.getEventQueue(streamName)) {
      data.addAll(buffs);
    }
    return data;
  }

  @Override
  public void cleanup(FutureWaiter futures) {
    this.done = true;
    this.executor.shutdown();
    this.executor.shutdownNow();
  }
}
