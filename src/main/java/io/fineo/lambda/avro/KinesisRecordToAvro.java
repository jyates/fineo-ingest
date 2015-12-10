package io.fineo.lambda.avro;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.fineo.internal.customer.Malformed;
import io.fineo.schema.MapRecord;
import io.fineo.schema.avro.AvroSchemaBridge;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.zip.Deflater;


/**
 * Lamba function to transform a raw record to an avro schema.
 * <p>
 * Records that are parseable are sent to the Kinesis 'parsed' stream. There may be multiple
 * different types of records in the same event, but they will all be based on the {@link io
 * .fineo.internal.customer .BaseRecord}, allowing access to standard and new fields + mapping.
 * Each record can then be deserialized via the usual {@link org.apache.avro.file.DataFileReader}.
 * </p>
 * <p>
 * Records that are not parsable via the usual schema mechanisms are sent to the 'malformed
 * records' Firehose Kinesis stream.
 * </p>
 */
public class KinesisRecordToAvro {

  private static final Log LOG = LogFactory.getLog(KinesisRecordToAvro.class);
  /**
   * 10 less than the 'max' just to ensure we don't write over
   */
  public static final int MAX_BATCH_THRESHOLD = 490;
  private static final long FIREHOSE_CREATING_WAIT_MS = 500;
  private AmazonKinesisFirehoseClient firehoseClient;
  private FirehoseClientProperties props;
  private SchemaStore store;
  private KinesisProducer kinesis;

  public void handler(KinesisEvent event) throws IOException {
    setup();
    try {
      handleEvent(event);
    } catch (Exception e) {
      malformedEvent(event);
    }
  }

  @VisibleForTesting
  void handleEvent(KinesisEvent event) throws IOException {
    PutRecordBatchRequest malformedBatch = null;
    for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
      malformedBatch = flushIfNecessary(malformedBatch);

      LOG.trace("Got message");
      ByteBuffer data = rec.getKinesis().getData();
      data.mark();
      // parse out the json
      JSON configuredJson = JSON.std.with(JSON.Feature.READ_ONLY).with(JSON.Feature
        .USE_DEFERRED_MAPS);
      Map<String, Object> values =
        configuredJson.mapFrom(new ByteBufferBackedInputStream(rec.getKinesis().getData()));

      // parse out the necessary values
      MapRecord record = new MapRecord(values);
      String orgId = record.getStringByField(SchemaBuilder.ORG_ID_KEY);
      AvroSchemaBridge bridge = AvroSchemaBridge.create(store, record);
      if (bridge == null) {
        malformedBatch = addMalformedRecord(malformedBatch, data);
        continue;
      }

      // write the record to a ByteBuffer
      GenericData.Record outRecord = bridge.encode(new MapRecord(values));
      FirehoseWriter writer = new FirehoseWriter()
        .setCodec(CodecFactory.deflateCodec(Deflater.BEST_SPEED));
      // add the record
      this.kinesis.addUserRecord(props.getParsedStreamName(), orgId, writer.write(outRecord));
    }

    if (LOG.isDebugEnabled()) {
      int malformedBatchSize = malformedBatch == null ? 0 : malformedBatch.getRecords().size();
      LOG.debug(
        "write out the malformed (" + malformedBatchSize + ") records");
    }
    writeBatch(malformedBatch);

    LOG.debug("Waiting on kinesis to finish writing all records");
    kinesis.flushSync();
    LOG.debug("Finished writing record batches");
  }

  /**
   * Batches only support up to a certain limit, after which point they must be flushed.
   *
   * @param batch batch to check
   * @return the current batch or <tt>null</tt> if the batch has been flushed
   */
  private PutRecordBatchRequest flushIfNecessary(PutRecordBatchRequest batch) {
    if (batch != null && batch.getRecords().size() >= MAX_BATCH_THRESHOLD) {
      writeBatch(batch);
      return null;
    }
    return batch;
  }

  private void writeBatch(PutRecordBatchRequest batch) {
    if (batch == null) {
      return;
    }
    PutRecordBatchResult result = firehoseClient.putRecordBatch(batch);
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

  private void malformedEvent(KinesisEvent event) throws IOException {
    PutRecordBatchRequest malformedBatch = null;
    for (KinesisEvent.KinesisEventRecord record : event.getRecords()) {
      malformedBatch = addMalformedRecord(malformedBatch, record.getKinesis().getData());
    }
    LOG.trace("Putting message to firehose");
    firehoseClient.putRecordBatch(malformedBatch);
    LOG.trace("Successfully put message to firehose");
  }

  private PutRecordBatchRequest addMalformedRecord(PutRecordBatchRequest batch, ByteBuffer data)
    throws IOException {
    // convert the data into a malformed record
    Malformed mal = Malformed.newBuilder().setRecordContent(data).build();
    // write it out into a new bytebuffer that we can read
    FirehoseWriter writer = new FirehoseWriter();
    ByteBuffer encoded = writer.write(mal);
    return addRecordToBatch(batch, props::getFirehoseMalformedStreamName, encoded);
  }

  public PutRecordBatchRequest addRecordToBatch(PutRecordBatchRequest batch,
    Supplier<String> streamNameGenerator, ByteBuffer data) {
    if (batch == null) {
      batch = new PutRecordBatchRequest()
        .withDeliveryStreamName(streamNameGenerator.get())
        .withRecords(Lists.newArrayList());
    }
    batch.getRecords().add(new Record().withData(data));
    return batch;
  }

  private void setup() throws IOException {
    props = FirehoseClientProperties.load();
    this.store = props.createSchemaStore();

    KinesisProducerConfiguration conf = new KinesisProducerConfiguration()
      .setCustomEndpoint(props.getKinesisEndpoint());
    this.kinesis = new KinesisProducer(conf);

    firehoseClient = new AmazonKinesisFirehoseClient();
    firehoseClient.setEndpoint(props.getFirehoseUrl());
    checkHoseStatus(props.getFirehoseMalformedStreamName());
  }

  private void checkHoseStatus(String deliveryStreamName) {
    DescribeDeliveryStreamRequest describeHoseRequest = new DescribeDeliveryStreamRequest()
      .withDeliveryStreamName(deliveryStreamName);
    DescribeDeliveryStreamResult describeHoseResult = null;
    String status = "";
    try {
      describeHoseResult = firehoseClient.describeDeliveryStream(describeHoseRequest);
      status = describeHoseResult.getDeliveryStreamDescription().getDeliveryStreamStatus();
    } catch (Exception e) {
      LOG.error("Firehose " + deliveryStreamName + " Not Existent", e);
      throw new RuntimeException(e);
    }
    if (status.equalsIgnoreCase("ACTIVE")) {
      LOG.debug("Firehose ACTIVE " + deliveryStreamName);
      //return;
    } else if (status.equalsIgnoreCase("CREATING")) {
      LOG.debug("Firehose CREATING " + deliveryStreamName);
      try {
        Thread.sleep(FIREHOSE_CREATING_WAIT_MS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      checkHoseStatus(deliveryStreamName);
    } else {
      LOG.debug("Status = " + status);
    }
  }

  @VisibleForTesting
  public void setupForTesting(FirehoseClientProperties props, AmazonKinesisFirehoseClient client,
    SchemaStore store, KinesisProducer producer) {
    this.props = props;
    this.firehoseClient = client;
    this.store = store;
    this.kinesis = producer;
  }
}
