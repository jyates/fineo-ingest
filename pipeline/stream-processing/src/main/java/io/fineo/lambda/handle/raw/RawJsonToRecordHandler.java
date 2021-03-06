package io.fineo.lambda.handle.raw;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.handle.TenantBoundFineoException;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.schema.MapRecord;
import io.fineo.schema.store.AvroSchemaEncoder;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static io.fineo.etl.FineoProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME;

/**
 * Handle converting raw JSON records to avro encoded values
 */
public class RawJsonToRecordHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RawJsonToRecordHandler.class);
  private final IKinesisProducer convertedRecords;
  private final String stream;
  private final SchemaStore store;

  @Inject
  public RawJsonToRecordHandler(@Named(KINESIS_PARSED_RAW_OUT_STREAM_NAME) String archiveStream,
    SchemaStore store, IKinesisProducer kinesis) {
    this.stream = archiveStream;
    this.store = store;
    this.convertedRecords = kinesis;
    LOG.trace("created json record handler");
  }

  public void handle(Map<String, Object> values) throws IOException {
    // parse out the necessary values
    MapRecord record = new MapRecord(values);
    LOG.trace("got record: {}", record);
    // this is an ugly reach into the bridge logic for the org ID, specially as we pull it out
    // when we create the schema bridge, but that requires a bit more refactoring than I want
    // to do right now for the schema bridge. Maybe an easy improvement later.
    String orgId = record.getStringByField(AvroSchemaProperties.ORG_ID_KEY);
    // sometimes this throws illegal argument, e.g. record not valid, so we fall back on the
    // error handler
    try {
      AvroSchemaEncoder bridge = AvroSchemaEncoder.create(store, record);
      LOG.trace("Got the encoder");

      // write the record to a ByteBuffer
      GenericRecord outRecord = bridge.encode();
      LOG.trace("Encoded the record {}", outRecord);
      // add the record
      this.convertedRecords.add(stream, orgId, outRecord);
      LOG.trace("Wrote the record");
    } catch (Exception e) {
      if (orgId == null) {
        throw new RuntimeException("No tenant id (API Key) found!", e);
      }
      throw new TenantBoundFineoException("Failed to apply schema for record: " + e.getMessage(), e,
          orgId, -1);
    }
  }

  public MultiWriteFailures<GenericRecord, ?> commit() {
    LOG.trace("Flushing converted records to kinesis");
    return convertedRecords.flush();
  }
}
