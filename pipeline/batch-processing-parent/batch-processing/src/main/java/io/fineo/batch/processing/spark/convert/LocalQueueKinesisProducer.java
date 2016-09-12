package io.fineo.batch.processing.spark.convert;

import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.kinesis.IKinesisProducer;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;

public class LocalQueueKinesisProducer implements IKinesisProducer {

  private Queue<GenericRecord> records = new LinkedTransferQueue<>();

  @Override
  public void add(String stream, String partitionKey, GenericRecord data) throws IOException {
    records.add(data);
  }

  public Queue<GenericRecord> getRecords() {
    return records;
  }

  @Override
  public MultiWriteFailures flush() {
    return new MultiWriteFailures<>(Collections.emptyList());
  }
}
