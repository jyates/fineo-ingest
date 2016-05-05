package io.fineo.lambda.firehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Wrapper around a FirehoseBatchWriter to manage things like flushing batches before they get
 * too large, retrying on failed adds, etc. (similar to the Kinesis Producer Library).
 */
public class FirehoseBatchWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FirehoseBatchWriter.class);
  /**
   * 10 less than the 'max' just to ensure we don't write over
   */
  public static final int MAX_BATCH_THRESHOLD = 490;

  private final Function<ByteBuffer, ByteBuffer> converter;
  private final AmazonKinesisFirehoseClient client;
  private final String streamName;
  private PutRecordBatchRequest batch = null;

  public FirehoseBatchWriter(String name, AmazonKinesisFirehoseClient client,
    Function<ByteBuffer, ByteBuffer> converter) {
    this.converter = converter;
    this.client = client;
    this.streamName = name;
    FirehoseUtils.checkHoseStatus(client, streamName);
  }

  public void addToBatch(ByteBuffer record) {
    batch = flushIfNecessary();
    ByteBuffer data = converter.apply(record);
    batch = addRecordToBatch(batch, data);
  }

  public void flush() throws IOException {
    if (LOG.isDebugEnabled()) {
      int malformedBatchSize = batch == null ? 0 : batch.getRecords().size();
      LOG.debug(
        "writing out " + malformedBatchSize + " records to " + this.streamName);
    }
    writeBatch(this.batch);
  }

  /**
   * Batches only support up to a certain limit, after which point they must be flushed.
   *
   * @return the current batch or <tt>null</tt> if the batch has been flushed
   */
  private PutRecordBatchRequest flushIfNecessary() {
    if (batch != null && batch.getRecords().size() >= MAX_BATCH_THRESHOLD) {
      writeBatch(batch);
      return null;
    }
    return batch;
  }

  private PutRecordBatchRequest addRecordToBatch(PutRecordBatchRequest batch, ByteBuffer data) {
    if (batch == null) {
      batch = new PutRecordBatchRequest()
        .withDeliveryStreamName(this.streamName)
        .withRecords(Lists.newArrayList());
    }
    batch.getRecords().add(new Record().withData(data));
    return batch;
  }

  private void writeBatch(PutRecordBatchRequest batch) {
    if (batch == null) {
      return;
    }
    PutRecordBatchResult result = client.putRecordBatch(batch);
    int count = result.getFailedPutCount();
    if (count == 0) {
      return;
    }
    // retry the batch with the given records
    List<Record> retries = new ArrayList<>(count);
    for (int i = 0; i < result.getRequestResponses().size(); i++) {
      PutRecordBatchResponseEntry entry = result.getRequestResponses().get(i);
      if (entry.getErrorCode() != null) {
        LOG.error("Failed to write: " + entry);
        retries.add(batch.getRecords().get(i));
      }
    }
    assert retries.size() > 0;
    batch.setRecords(retries);
    writeBatch(batch);
  }
}
