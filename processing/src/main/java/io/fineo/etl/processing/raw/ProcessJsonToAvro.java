package io.fineo.etl.processing.raw;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.LongSerializationPolicy;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.inject.Inject;
import io.fineo.etl.processing.BaseProcessor;
import io.fineo.etl.processing.Message;
import io.fineo.etl.processing.OutputWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.schema.MapRecord;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Processor that converts raw, client provided JSON into Avro encoded message(s). Errors are
 * written to AWS Firehose.
 */
public class ProcessJsonToAvro extends BaseProcessor<Void> {

  private static final Log LOG = LogFactory.getLog(ProcessJsonToAvro.class);
  private final SchemaStore store;
  private final JsonFactory jsonFactory;

  @Inject
  public ProcessJsonToAvro(Supplier<FirehoseBatchWriter> processingErrors,
    Supplier<FirehoseBatchWriter> commitErrors, OutputWriter<Message<Void>> writer,
    SchemaStore store) {
    super(processingErrors, commitErrors, writer);
    this.store = store;
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.setCodec(new ObjectMapper());
    this.jsonFactory = jsonFactory;
  }

  @Override
  protected void process(String json, OutputWriter<Message<Void>> writer) throws IOException {
//    JsonParser parse = new JsonFactory().createParser(json);
    LOG.trace("Raw json: " + json);
    for (Map<String, Object> values : parse(json)) {
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
      writer.write(new Message<>(null, bridge.encode(new MapRecord(values))));
    }
  }

  private List<Map<String, Object>> parse(String json) throws IOException {
    JsonParser jp = jsonFactory.createParser(new ByteArrayInputStream(json.getBytes()));
    com.fasterxml.jackson.core.JsonToken token;
    List<Map<String, Object>> messages = new ArrayList<>();
    while ((token = jp.nextToken()) != null) {
      switch (token) {
        case START_OBJECT:
          messages.add(jp.readValueAs(Map.class));
          break;
      }
    }
    return messages;
  }
}
