package io.fineo.lambda.e2e.resources.lambda;

import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.StreamProducer;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.kinesis.MockKinesisStreams;
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
 * Connect lambda to kinesis streams
 */
public class LocalLambdaKinesisConnector extends LambdaKinesisConnector<IngestUtil.Lambda> {

  private MockKinesisStreams kinesis;

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  public LocalLambdaKinesisConnector(Map<String, List<IngestUtil.Lambda>> streamToLambdaMapping,
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
  public void connect(LambdaClientProperties props) throws IOException {
    // have local streams
    this.kinesis = new MockKinesisStreams();

    // ensure that the outputs can actually write back to kinesis
    for (List<IngestUtil.Lambda> lambdas : this.mapping.values()) {
      for (IngestUtil.Lambda lambda : lambdas) {
        Object func = lambda.getFunction();
        if (func instanceof StreamProducer) {
          ((StreamProducer) func).setDownstreamForTesting(kinesis.getProducer());
        }
      }
    }

    executor.execute(() -> {
      Map<String, BlockingQueue<List<ByteBuffer>>> streams = new HashMap<>();
      for (String stream : mapping.keySet()) {
        streams.put(stream, kinesis.getEventQueue(stream, true));
      }

      while (true) {
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
    for (List<ByteBuffer> buffs : this.kinesis.getEventQueue(streamName, true)) {
      data.addAll(buffs);
    }
    return data;
  }

  @Override
  public void cleanup(FutureWaiter futures) {
    this.executor.shutdown();
    this.executor.shutdownNow();
  }
}
