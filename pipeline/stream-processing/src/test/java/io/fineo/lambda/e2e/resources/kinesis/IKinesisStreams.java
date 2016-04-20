package io.fineo.lambda.e2e.resources.kinesis;

import io.fineo.lambda.kinesis.IKinesisProducer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Higher level interface for interacting with kinesis streams
 */
public interface IKinesisStreams {
  IKinesisProducer getProducer();

  void submit(String streamName, ByteBuffer data);

  BlockingQueue<List<ByteBuffer>> getEventQueue(String stream);

  void setup(String stream);

  Iterable<String> getStreamNames();
}
