package io.fineo.lambda.kinesis;

import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.FlushResponse;
import io.fineo.schema.Pair;

import java.util.List;
import java.util.function.Consumer;

/**
 * Light rapper around a {@link SimpleKinesisProducer} that just every writes to a single stream
 */
public class SingleStreamKinesisProducer {

  private final String stream;
  private final SimpleKinesisProducer producer;

  public SingleStreamKinesisProducer(String stream, SimpleKinesisProducer producer) {
    this.stream = stream;
    this.producer = producer;
  }

  /**
   * Write a source object, encoded as the specified bytes, to the given partition key. Source
   * object is then referenced any failures so you can handle them with some decent logic.
   *
   * @param partitionKey partitioning key for the kinesis shard selection
   * @param buff         raw data to send
   * @param source       original object
   */
  public void write(String partitionKey, byte[] buff, Object source) {
    producer.write(stream, partitionKey, buff, source);
  }

  public void write(String partitionKey, List<Pair<byte[], Object>> sources,
    Consumer<AwsAsyncRequest<List<Object>, PutRecordsRequest>> success,
    Consumer<AwsAsyncRequest<List<Object>, PutRecordsRequest>> failure) {
    producer.write(stream, partitionKey, sources, success, failure);
  }

  public <T> FlushResponse<T, PutRecordRequest> flushSingleEvent() {
    return (FlushResponse<T, PutRecordRequest>) producer.flushSingleEvent();
  }

  public FlushResponse<List<Object>, PutRecordsRequest> flushEvents() {
    return producer.flushRecords();
  }
}
