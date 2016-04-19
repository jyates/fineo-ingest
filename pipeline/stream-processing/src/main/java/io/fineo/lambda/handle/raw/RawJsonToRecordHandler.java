package io.fineo.lambda.handle.raw;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.schema.MapRecord;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;

import static io.fineo.lambda.configure.legacy.LambdaClientProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME;

/**
 * Handle converting raw JSON records to avro encoded values
 */
public class RawJsonToRecordHandler {

  private static final Log LOG = LogFactory.getLog(RawJsonToRecordHandler.class);
  private final KinesisProducer convertedRecords;
  private final String stream;
  private final SchemaStore store;

  @Inject
  public RawJsonToRecordHandler(@Named(KINESIS_PARSED_RAW_OUT_STREAM_NAME) String archiveStream,
    SchemaStore store, KinesisProducer kinesis) {
    this.stream = archiveStream;
    this.store = store;
    this.convertedRecords = kinesis;
  }

  public void handle(Map<String, Object> values) throws IOException {
    // parse out the necessary values
    MapRecord record = new MapRecord(values);
    LOG.trace("got record");
    // this is an ugly reach into the bridge, logic for the org ID, specially as we pull it out
    // when we create the schema bridge, but that requires a bit more refactoring than I want
    // to do right now for the schema bridge. Maybe an easy improvement later.
    String orgId = record.getStringByField(AvroSchemaEncoder.ORG_ID_KEY);
    // sometimes this throws illegal argument, e.g. record not valid, so we fall back on the
    // error handler
    AvroSchemaEncoder bridge = AvroSchemaEncoder.create(store, record);
    LOG.trace("Got the encoder");

    // write the record to a ByteBuffer
    GenericRecord outRecord = bridge.encode(new MapRecord(values));
    LOG.trace("Encoded the record");
    // add the record
    this.convertedRecords.add(stream, orgId, outRecord);
    LOG.trace("Wrote the record");
  }

  public MultiWriteFailures<GenericRecord> commit() {
    LOG.trace("Flushing converted records to kinesis");
    return convertedRecords.flush();
  }
}
