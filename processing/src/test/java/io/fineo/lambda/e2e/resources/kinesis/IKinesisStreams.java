package io.fineo.lambda.e2e.resources.kinesis;

import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.schema.Pair;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

/**
 * Higher level interface for interacting with kinesis streams
 */
public interface IKinesisStreams {
  KinesisProducer getProducer();

  void submit(String streamName, ByteBuffer data);

  BlockingQueue<List<ByteBuffer>> getEventQueue(String stream, boolean start);

  void setup(String stream);
}
