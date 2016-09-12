package io.fineo.lambda.kinesis;

import io.fineo.lambda.aws.MultiWriteFailures;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 *
 */
public interface IKinesisProducer {
  void add(String stream, String partitionKey, GenericRecord data) throws IOException;

  MultiWriteFailures<GenericRecord, ?> flush();
}
