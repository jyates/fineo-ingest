package io.fineo.etl.processing.raw;

import com.fasterxml.jackson.jr.ob.JSON;
import io.fineo.etl.processing.BaseProcessor;
import io.fineo.etl.processing.Message;
import io.fineo.etl.processing.OutputWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.schema.MapRecord;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Processor that converts raw, client provided JSON into Avro encoded message(s). Errors are
 * written to AWS Firehose.
 */
public class ProcessJsonToAvro extends BaseProcessor<Void> {

  private static final Log LOG = LogFactory.getLog(ProcessJsonToAvro.class);
  private final JSON parser;
  private final SchemaStore store;

  public ProcessJsonToAvro(Supplier<FirehoseBatchWriter> processingErrors,
    Supplier<FirehoseBatchWriter> commitErrors, OutputWriter<Message<Void>> writer,
    SchemaStore store) {
    super(processingErrors, commitErrors, writer);
    this.store = store;
    this.parser = JSON.std.with(JSON.Feature.READ_ONLY).with(JSON.Feature.USE_DEFERRED_MAPS);
  }

  public Message<Void> process(String json) throws IOException {
    Map<String, Object> values = parser.mapFrom(json);
    LOG.trace("Parsed json: " + values);
    // parse out the necessary values
    MapRecord record = new MapRecord(values);
    // this is an ugly reach into the bridge, logic for the org ID, specially as we pull it out
    // when we create the schema bridge, but that requires a bit more refactoring than I want
    // to do right now for the schema bridge. Maybe an easy improvement later.
    String orgId = record.getStringByField(AvroSchemaEncoder.ORG_ID_KEY);
    // sometimes this throws illegal argument, e.g. record not valid, so we fall back on the
    // error handler
    AvroSchemaEncoder bridge = AvroSchemaEncoder.create(store, record);
    LOG.trace("Got the encoder");
    // encode the record
    return new Message<>(null, bridge.encode(new MapRecord(values)));
  }
}
