package io.fineo.lambda.avro;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.internal.customer.Malformed;
import io.fineo.lambda.StreamProducer;
import io.fineo.lambda.storage.TestableLambda;
import io.fineo.schema.MapRecord;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;


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
public class LambdaRawRecordToAvro implements StreamProducer, TestableLambda {

  private static final Log LOG = LogFactory.getLog(LambdaRawRecordToAvro.class);
  private FirehoseBatchWriter malformedRecords;
  private LambdaClientProperties props;
  private SchemaStore store;
  private KinesisProducer convertedRecords;

  public void handler(KinesisEvent event) throws IOException {
    setup();
    try {
      handleEventInternal(event);
    } catch (Exception e) {
      malformedEvent(event);
    }
    LOG.info("Finished!");
  }

  @VisibleForTesting
  @Override
  public void handleEventInternal(KinesisEvent event) throws IOException {
    LOG.info("Entering handler");
    for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
      LOG.trace("Got message");
      ByteBuffer data = rec.getKinesis().getData();
      data.mark();
      // parse out the json
      JSON configuredJson = JSON.std.with(JSON.Feature.READ_ONLY).with(JSON.Feature
        .USE_DEFERRED_MAPS);
      Map<String, Object> values =
        configuredJson.mapFrom(new ByteBufferBackedInputStream(rec.getKinesis().getData()));
      LOG.info("Parsed json: "+values);
      // parse out the necessary values
      MapRecord record = new MapRecord(values);
      // this is an ugly reach into the bridge, logic for the org ID, specially as we pull it out
      // when we create the schema bridge, but that requires a bit more refactoring than I want
      // to do right now for the schema bridge. Maybe an easy improvement later.
      String orgId = record.getStringByField(AvroSchemaEncoder.ORG_ID_KEY);
      AvroSchemaEncoder bridge;
      try {
        bridge = AvroSchemaEncoder.create(store, record);
      } catch (IllegalArgumentException e) {
        malformedRecords.addToBatch(data);
        continue;
      }
      LOG.info("Got the encoder");

      // write the record to a ByteBuffer
      GenericRecord outRecord = bridge.encode(new MapRecord(values));
      LOG.info("Encoded the record");
      FirehoseRecordWriter writer = FirehoseRecordWriter.create();
      // add the record
      this.convertedRecords.addUserRecord(props.getParsedStreamName(), orgId,
        writer.write(outRecord));
      LOG.info("Wrote the record");
    }

    LOG.info("Flushing malformed records");
    malformedRecords.flush();
    LOG.info("Flushed malformed records");

    LOG.debug("Waiting on kinesis to finish writing all converted records");
    convertedRecords.flushSync();
    LOG.debug("Finished writing record batches");
  }

  private void malformedEvent(KinesisEvent event) throws IOException {
    for (KinesisEvent.KinesisEventRecord record : event.getRecords()) {
      malformedRecords.addToBatch(record.getKinesis().getData());
    }
    LOG.trace("Putting message to firehose");
    malformedRecords.flush();
    LOG.trace("Successfully put message to firehose");
  }

  private Function<ByteBuffer, ByteBuffer> transform = data -> {
    // convert the data into a malformed record
    Malformed mal = Malformed.newBuilder().setRecordContent(data).build();
    // write it out into a new bytebuffer that we can read
    FirehoseRecordWriter writer = new FirehoseRecordWriter();
    try {
      return writer.write(mal);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  };


  private void setup() throws IOException {
    LOG.info("Setting up");
    props = LambdaClientProperties.load();
    LOG.info("Creating store");
    this.store = props.createSchemaStore();

    LOG.info("Setting up producer");
    KinesisProducerConfiguration conf = new KinesisProducerConfiguration()
      .setCustomEndpoint(props.getKinesisEndpoint());
    this.convertedRecords = new KinesisProducer(conf);

    LOG.info("Setting up batch writer");
    malformedRecords = new FirehoseBatchWriter(props, transform, props
      .getFirehoseMalformedStreamName());
  }

  @VisibleForTesting
  public void setupForTesting(LambdaClientProperties props, AmazonKinesisFirehoseClient client,
    SchemaStore store, KinesisProducer producer, FirehoseBatchWriter malformed) {
    this.props = props;
    this.malformedRecords =
      malformed != null?
      malformed:
      new FirehoseBatchWriter(transform, props.getFirehoseMalformedStreamName(), client);
    this.store = store;
    setDownstreamForTesting(producer);
  }

  @Override
  public void setDownstreamForTesting(KinesisProducer producer) {
    this.convertedRecords = producer;
  }
}
