package io.fineo.lambda;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.etl.processing.JsonParser;
import io.fineo.internal.customer.Malformed;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.kinesis.KinesisProducer;
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
import java.util.function.Supplier;


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
public class LambdaRawRecordToAvro extends IngestBaseLambda implements StreamProducer {

  private static final Log LOG = LogFactory.getLog(LambdaRawRecordToAvro.class);
  private final JsonParser parser;
  private SchemaStore store;
  private KinesisProducer convertedRecords;

  public LambdaRawRecordToAvro() {
    super(LambdaClientProperties.RAW_PREFIX);
    this.parser = new JsonParser();
  }

  @VisibleForTesting
  @Override
  public void handleEvent(KinesisEvent.KinesisEventRecord rec) throws IOException {
    for (Map<String, Object> values : parser
      .parse(new ByteBufferBackedInputStream(rec.getKinesis().getData()))) {
      LOG.trace("Parsed json: " + values);
      // parse out the necessary values
      MapRecord record = new MapRecord(values);
      LOG.trace("got record");
      // this is an ugly reach into the bridge, logic for the org ID, specially as we pull it out
      // when we create the schema bridge, but that requires a bit more refactoring than I want
      // to do right now for the schema bridge. Maybe an easy improvement later.
      String orgId = record.getStringByField(AvroSchemaEncoder.ORG_ID_KEY);
      LOG.trace("got org id" + orgId + " for key: " + AvroSchemaEncoder.ORG_ID_KEY);
      // sometimes this throws illegal argument, e.g. record not valid, so we fall back on the
      // error handler
      AvroSchemaEncoder bridge = AvroSchemaEncoder.create(store, record);
      LOG.trace("Got the encoder");

      // write the record to a ByteBuffer
      GenericRecord outRecord = bridge.encode(new MapRecord(values));
      LOG.trace("Encoded the record");
      // add the record
      this.convertedRecords.add(props.getRawToStagedKinesisStreamName(), orgId, outRecord);
      LOG.trace("Wrote the record");
    }
  }

  public MultiWriteFailures<GenericRecord> commit() throws IOException {
    LOG.trace("Flushing converted records to kinesis");
    return convertedRecords.flush();
  }

  @Override
  protected void setup() throws IOException {
    LOG.debug("Creating store");
    this.store = props.createSchemaStore();

    LOG.debug("Setting up producer");
    this.convertedRecords =
      new KinesisProducer(props.getKinesisClient(), props.getKinesisRetries());
  }


  @Override
  protected Supplier<FirehoseBatchWriter> getProcessingErrorStream() {
    return this.lazyFirehoseBatchWriter(props.getFirehoseStreamName(this.phaseName,
      LambdaClientProperties.StreamType.PROCESSING_ERROR), transform);
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

  @VisibleForTesting
  public void setupForTesting(LambdaClientProperties props, SchemaStore store,
    KinesisProducer producer, FirehoseBatchWriter archive, FirehoseBatchWriter processingErrors,
    FirehoseBatchWriter commitFailure) {
    super.setupForTesting(archive, processingErrors, commitFailure);
    this.props = props;
    this.store = store;
    setDownstreamForTesting(producer);
  }

  @Override
  public void setDownstreamForTesting(KinesisProducer producer) {
    this.convertedRecords = producer;
  }
}
