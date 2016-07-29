package io.fineo.lambda.e2e.aws.kinesis;

import com.google.inject.Injector;
import io.fineo.lambda.e2e.manager.IKinesisStreams;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.lambda.util.run.FutureWaiter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class DelegateAwsKinesisStreamManager implements IKinesisStreams {
  private IKinesisStreams delegate;

  @Override
  public void init(Injector injector) {
    this.delegate = injector.getInstance(KinesisStreamManager.class);
  }

  @Override
  public void cleanup(FutureWaiter waiter) {
    this.delegate.cleanup(waiter);
  }

  @Override
  public IKinesisProducer getProducer() {
    return delegate.getProducer();
  }

  @Override
  public void submit(String streamName, ByteBuffer data) {
    delegate.submit(streamName, data);
  }

  @Override
  public BlockingQueue<List<ByteBuffer>> getEventQueue(String stream) {
    return delegate.getEventQueue(stream);
  }

  @Override
  public void setup(String stream) {
    delegate.setup(stream);
  }

  @Override
  public Iterable<String> getStreamNames() {
    return delegate.getStreamNames();
  }
}
