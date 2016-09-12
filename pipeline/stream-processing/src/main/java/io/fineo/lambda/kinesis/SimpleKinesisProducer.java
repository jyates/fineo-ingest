package io.fineo.lambda.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.google.inject.Inject;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.aws.FlushResponse;
import io.fineo.schema.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SimpleKinesisProducer {

  private final long MAX_BYTES = 5000000;
  private final int MAX_EVENTS_PER_REQUEST = 500;
  private final long MAX_EVENT_SIZE = 1000000 - 512;

  private final AwsAsyncSubmitter<PutRecordRequest, PutRecordResult, Object> submitter;
  private final AwsAsyncSubmitter<PutRecordsRequest, PutRecordsResult, List<Object>>
    submitterEvents;

  @Inject
  public SimpleKinesisProducer(AmazonKinesisAsyncClient kinesisClient,
    long kinesisRetries) {
    this.submitter = new AwsAsyncSubmitter<>(kinesisRetries, kinesisClient::putRecordAsync);
    this.submitterEvents = new AwsAsyncSubmitter<>(kinesisRetries, kinesisClient::putRecordsAsync);
  }

  public void write(String stream, String partitionKey, byte[] buff, Object source) {
    checkEventSize(buff);
    PutRecordRequest request =
      new PutRecordRequest().withStreamName(stream)
                            .withData(ByteBuffer.wrap(buff))
                            .withPartitionKey(partitionKey);
    submitter.submit(new AwsAsyncRequest<>(source, request));
  }

  private void checkEventSize(byte[] buff) {
    if (buff.length > MAX_EVENT_SIZE) {
      throw new IllegalArgumentException(
        "Events must be less than " + MAX_EVENT_SIZE + " bytes when "
        + "serialized to UTF-8 strings. Your event was: " + buff.length + " bytes.");
    }
  }

  public void write(String stream, String partitionKey, List<Pair<byte[], Object>> sources) {
    Batch current = new Batch();
    List<Batch> batches = new ArrayList<>();
    for (Pair<byte[], Object> source : sources) {
      checkEventSize(source.getKey());
      if (!current.tryAdd(partitionKey, source.getKey(), source.getValue())) {
        batches.add(current);
        current = new Batch();
        assert current.tryAdd(partitionKey, source.getKey(), source.getValue()) :
          "Fresh batch did not support adding key!";
      }
    }
    if (!current.empty()) {
      batches.add(current);
    }

    for (Batch batch : batches) {
      submitBatchRecords(stream, batch.records, batch.events);
    }
  }

  private class Batch {
    List<PutRecordsRequestEntry> records = new ArrayList<>();
    List<Object> events = new ArrayList<>();
    private long size = 0;

    public boolean tryAdd(String partitionKey, byte[] data, Object source) {
      if (events.size() == MAX_EVENTS_PER_REQUEST || size + data.length > MAX_BYTES) {
        return false;
      }
      events.add(source);
      records.add(new PutRecordsRequestEntry().withData(ByteBuffer.wrap(data))
                                              .withPartitionKey(partitionKey));
      size += data.length;
      return true;
    }

    public boolean empty() {
      return this.records.size() > 0;
    }
  }

  private void submitBatchRecords(String stream,
    List<PutRecordsRequestEntry> records, List<Object> events) {
    PutRecordsRequest request = new PutRecordsRequest()
      .withStreamName(stream)
      .withRecords(records);
    submitterEvents.submit(new AwsAsyncRequest<>(events, request));
  }

  public FlushResponse<Object, PutRecordRequest> flushSingleEvent() {
    return submitter.flushRequests();
  }

  public FlushResponse<List<Object>, PutRecordsRequest> flushRecords() {
    return submitterEvents.flushRequests();
  }
}
