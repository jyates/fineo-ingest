package io.fineo.lambda.firehose;


import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DeliveryStreamDescription;
import com.amazonaws.services.kinesisfirehose.model.DeliveryStreamStatus;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestFirehoseBatchWriter {

  @Test
  public void testAddAndFlush() throws Exception {
    writeAndVerifyRecords(1);
  }

  @Test
  public void testMultipleRecords() throws Exception {
    writeAndVerifyRecords(2);
  }

  private void writeAndVerifyRecords(int recordCount) throws Exception {
    writeReadRecordsOneBatch(createData(recordCount));
  }

  private ByteBuffer[] createData(int recordCount) {
    ByteBuffer[] data = new ByteBuffer[recordCount];
    for (int i = 0; i < recordCount; i++) {
      data[i] = ByteBuffer.wrap(UUID.randomUUID().toString().getBytes());
    }
    return data;
  }

  private void writeReadRecordsOneBatch(ByteBuffer... data) throws IOException {
    String stream = "streamname";
    AmazonKinesisFirehoseClient client = getMockActiveClient();
    FirehoseBatchWriter writer = new FirehoseBatchWriter(stream, client, ByteBuffer::duplicate);

    for (ByteBuffer datum : data) {
      writer.addToBatch(datum);
    }
    verifyActiveClient(client);

    PutRecordBatchResult result = Mockito.mock(PutRecordBatchResult.class);
    List<PutRecordBatchRequest> requests = new ArrayList<>();
    Mockito.when(client.putRecordBatch(Mockito.any())).then(call -> {
      requests.add((PutRecordBatchRequest) call.getArguments()[0]);
      return result;
    });

    writer.flush();
    Mockito.verify(client).putRecordBatch(Mockito.any());
    assertEquals(1, requests.size());
    assertEquals(stream, requests.get(0).getDeliveryStreamName());
    List<Record> records = requests.get(0).getRecords();
    verifyRecordsMatchData(data, records);
  }

  @Test
  public void flushNearLimits() throws Exception {
    final int batchThreshold = 490;
    ByteBuffer[] data = createData(500);
    String stream = "streamname";
    AmazonKinesisFirehoseClient client = getMockActiveClient();
    FirehoseBatchWriter writer = new FirehoseBatchWriter(stream, client, ByteBuffer::duplicate);
    verifyActiveClient(client);

    PutRecordBatchResult result = Mockito.mock(PutRecordBatchResult.class);
    List<PutRecordBatchRequest> requests = new ArrayList<>();
    Mockito.when(client.putRecordBatch(Mockito.any())).then(call -> {
      requests.add((PutRecordBatchRequest) call.getArguments()[0]);
      return result;
    });

    for (ByteBuffer datum : data) {
      writer.addToBatch(datum);
    }
    // one call to put records for the first batch
    Mockito.verify(client).putRecordBatch(Mockito.any());
    assertEquals(1, requests.size());
    List<Record> records = requests.get(0).getRecords();
    assertEquals(batchThreshold, records.size());
    for (int i = 0; i < batchThreshold; i++) {
      assertEquals(data[i], records.get(i).getData());
    }
    requests.clear();

    // second batch get flushed
    writer.flush();
    Mockito.verify(client, Mockito.times(2)).putRecordBatch(Mockito.any());
    assertEquals(1, requests.size());
    assertEquals(stream, requests.get(0).getDeliveryStreamName());
    records = requests.get(0).getRecords();
    assertEquals(data.length - batchThreshold, records.size());
    for (int i = 0; i < data.length - batchThreshold; i++) {
      assertEquals(data[490 + i], records.get(i).getData());
    }
  }

  private AmazonKinesisFirehoseClient getMockActiveClient() {
    AmazonKinesisFirehoseClient client = Mockito.mock(AmazonKinesisFirehoseClient.class);
    DescribeDeliveryStreamResult describeResult = new DescribeDeliveryStreamResult();
    DeliveryStreamDescription desc = new DeliveryStreamDescription();
    desc.setDeliveryStreamStatus(DeliveryStreamStatus.ACTIVE);
    describeResult.setDeliveryStreamDescription(desc);
    Mockito.when(client.describeDeliveryStream(Mockito.any())).thenReturn(describeResult);
    return client;
  }

  private void verifyActiveClient(AmazonKinesisFirehoseClient mock) {
    Mockito.verify(mock).describeDeliveryStream(Mockito.any());
  }

  @Test
  public void retries() throws Exception {
    int recordCount = 5;
    int failureCount = 2;
    ByteBuffer[] data = createData(recordCount);

    AmazonKinesisFirehoseClient client = getMockActiveClient();

    // setup the responses as 2 failures and then all success
    PutRecordBatchResult result = new PutRecordBatchResult();
    PutRecordBatchResponseEntry success = new PutRecordBatchResponseEntry()
      .withRecordId("written-id");
    PutRecordBatchResponseEntry failure =
      new PutRecordBatchResponseEntry().withErrorCode("1").withErrorMessage("Some error");
    result.withRequestResponses(success, failure, success, success, failure);
    result.setFailedPutCount(failureCount);

    PutRecordBatchResult secondResult = new PutRecordBatchResult();
    secondResult.withRequestResponses(success, success);
    secondResult.setFailedPutCount(0);

    List<PutRecordBatchResult> results = Lists.newArrayList(result, secondResult);
    List<Pair<PutRecordBatchRequest, List<Record>>> requests = new ArrayList<>();
    Mockito.when(client.putRecordBatch(Mockito.any(PutRecordBatchRequest.class)))
           .then(invocation -> {
             PutRecordBatchRequest request = (PutRecordBatchRequest) invocation.getArguments()[0];
             List<Record> records1 = Lists.newArrayList(request.getRecords());
             requests.add(new ImmutablePair<> (request, records1));
             return results.remove(0);
           });

    String stream = "streamname";
    FirehoseBatchWriter writer = new FirehoseBatchWriter(stream, client, ByteBuffer::duplicate);
    verifyActiveClient(client);
    for (ByteBuffer datum : data) {
      writer.addToBatch(datum);
    }
    writer.flush();

    Mockito.verify(client, Mockito.times(2)).putRecordBatch(Mockito.any());
    assertEquals(2, requests.size());
    assertEquals(stream, requests.get(0).getKey().getDeliveryStreamName());
    assertEquals(stream, requests.get(1).getKey().getDeliveryStreamName());
    List<Record> records = requests.get(0).getValue();
    verifyRecordsMatchData(data, records);
    verifyRecordsMatchData(new ByteBuffer[]{data[1], data[4]}, requests.get(1).getValue());
  }

  private void verifyRecordsMatchData(ByteBuffer[] data, List<Record> records) {
    assertEquals(data.length, records.size());
    for (int i = 0; i < data.length; i++) {
      assertEquals(data[i], records.get(i).getData());
    }
  }
}
