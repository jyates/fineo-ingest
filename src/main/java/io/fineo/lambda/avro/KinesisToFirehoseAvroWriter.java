package io.fineo.lambda.avro;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.fineo.internal.customer.Metadata;
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
import java.util.Map;
import java.util.function.Supplier;
import java.util.zip.Deflater;


/**
 * Lamba function to transform a raw record to an avro schema and attach it to the firehose
 */
public class KinesisToFirehoseAvroWriter {

  private static final Log LOG = LogFactory.getLog(KinesisToFirehoseAvroWriter.class);
  private AmazonKinesisFirehoseClient firehoseClient;
  private FirehoseClientProperties props;
  private SchemaStore store;

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
    PutRecordBatchRequest batch = null;
    PutRecordBatchRequest malformedBatch = null;
    for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
      LOG.trace("Got message");
      ByteBuffer data = rec.getKinesis().getData();
      data.mark();
      // parse out the json
      JSON configuredJson = JSON.std.with(JSON.Feature.READ_ONLY).with(JSON.Feature
        .USE_DEFERRED_MAPS);
      Map<String, Object> values =
        configuredJson.mapFrom(new ByteBufferBackedInputStream(rec.getKinesis().getData()));

      // parse out the necessary values
      String orgID = (String) values.get(SchemaBuilder.ORG_ID_KEY);
      String machineType = (String) values.get(SchemaBuilder.ORG_METRIC_TYPE_KEY);

      if (orgID == null || machineType == null) {
        malformedBatch = addMalformedRecord(malformedBatch, data);
      }

      // load the schema for the record
      Metadata orgMetadata = store.getSchemaTypes(orgID);
      AvroSchemaBridge bridge = AvroSchemaBridge.create(orgMetadata, store, machineType);
      GenericData.Record outRecord = bridge.encode(new MapRecord(values));

      // write the record to a ByteBuffer
      FirehoseWriter writer = new FirehoseWriter().setCodec(CodecFactory.deflateCodec(Deflater
        .BEST_SPEED));
      // add the record
      batch = addCorrectlyFormedRecord(batch, writer.write(outRecord));
    }

    LOG.debug("write out the correct (" + batch.getRecords().size() + ") and malformed "
              + "(" + malformedBatch.getRecords().size() + ") records");
    firehoseClient.putRecordBatch(batch);
    firehoseClient.putRecordBatch(malformedBatch);
    LOG.debug("Finished writing record batches");
  }

  private void malformedEvent(KinesisEvent event) {
    PutRecordBatchRequest malformedBatch = null;
    for (KinesisEvent.KinesisEventRecord record : event.getRecords()) {
      malformedBatch = addMalformedRecord(malformedBatch, record.getKinesis().getData());
    }
    LOG.trace("Putting message to firehose");
    firehoseClient.putRecordBatch(malformedBatch);
    LOG.trace("Successfully put message to firehose");
  }

  private PutRecordBatchRequest addCorrectlyFormedRecord(PutRecordBatchRequest batch, ByteBuffer
    data) {
    return addRecordToBatch(batch, props::getFirehoseStreamName, data);
  }

  private PutRecordBatchRequest addMalformedRecord(PutRecordBatchRequest batch, ByteBuffer data) {
    return addRecordToBatch(batch, props::getFirehoseMalformedStreamName, data);
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

    firehoseClient = new AmazonKinesisFirehoseClient();
    firehoseClient.setEndpoint(props.getFirehoseUrl());
    checkHoseStatus(props.getFirehoseStreamName());
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
      System.out.println(e.getLocalizedMessage());
      LOG.error("Firehose " + deliveryStreamName + " Not Existent", e);
      throw new RuntimeException(e);
    }
    if (status.equalsIgnoreCase("ACTIVE")) {
      LOG.debug("Firehose ACTIVE " + deliveryStreamName);
      //return;
    } else if (status.equalsIgnoreCase("CREATING")) {
      LOG.debug("Firehose CREATING " + deliveryStreamName);
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      checkHoseStatus(deliveryStreamName);
    } else {
      LOG.debug("Status = " + status);
    }
  }
}
