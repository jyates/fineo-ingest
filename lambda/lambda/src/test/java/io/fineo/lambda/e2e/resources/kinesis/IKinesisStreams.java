package io.fineo.lambda.e2e.resources.kinesis;

import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.schema.Pair;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

/**
 * Higher level interface for interacting with kinesis streams
 */
public interface IKinesisStreams {
  KinesisProducer getProducer();

  void submit(String streamName, ByteBuffer data);

  Stream<Pair<String, List<ByteBuffer>>> events();

  List<ByteBuffer> getEvents(String stream, boolean start);
}
