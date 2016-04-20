package io.fineo.lambda.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.aws.MultiWriteFailures;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 * Similar amazon's {@link com.amazonaws.services.kinesis.producer.KinesisProducer}, but uses the
 * standard {@link com.amazonaws.services.kinesis.AmazonKinesisClient} to make requests, rather
 * than leveraging the KPL. This has two advantages:
 * <ol>
 * <li>Works in Lambda functions because it doesn't require access to /tmp</li>
 * <li>Has a much smaller startup time, a huge boon to Lambda functions</li>
 * </ol>
 * <p/>
 * All writes are accumulated until a call to {@link #flush()}, which is <b>blocks until all
 * requests have completed</b>. This is merely a simple wrapper around an {@link AwsAsyncSubmitter}
 *
 * @see AwsAsyncSubmitter for more information about thread safety
 */
public class KinesisProducer implements IKinesisProducer {
  private final AwsAsyncSubmitter<PutRecordRequest, PutRecordResult, GenericRecord> submitter;
  private final FirehoseRecordWriter converter;

  public KinesisProducer(AmazonKinesisAsyncClient kinesisClient, long kinesisRetries) {
    this.submitter = new AwsAsyncSubmitter<>(kinesisRetries, kinesisClient::putRecordAsync);
    this.converter = FirehoseRecordWriter.create();
  }

  @Override
  public void add(String stream, String partitionKey, GenericRecord data) throws IOException {
    PutRecordRequest request =
      new PutRecordRequest().withStreamName(stream)
                            .withData(converter.write(data))
                            .withPartitionKey(partitionKey);
    submitter.submit(new AwsAsyncRequest<>(data, request));
  }

  @Override
  public MultiWriteFailures<GenericRecord> flush() {
    return this.submitter.flush();
  }
}
