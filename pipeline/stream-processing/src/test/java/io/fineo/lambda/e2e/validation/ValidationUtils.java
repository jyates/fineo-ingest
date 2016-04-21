package io.fineo.lambda.e2e.validation;

import com.google.common.collect.Lists;
import io.fineo.lambda.util.SchemaUtil;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ValidationUtils {

  private static final Log LOG = LogFactory.getLog(ValidationUtils.class);

  public static ByteBuffer combine(List<ByteBuffer> data) {
    int size = data.stream().mapToInt(bb -> bb.remaining()).sum();
    ByteBuffer combined = ByteBuffer.allocate(size);
    data.forEach(bb -> combined.put(bb));
    combined.rewind();
    return combined;
  }

  static void empty(Function<List<ByteBuffer>, String> errorResult,
    List<ByteBuffer> records) {
    String readable = errorResult.apply(records);
    assertEquals("Found records: " + readable, Lists.newArrayList(), records);
  }

  public static Stream<Map.Entry<String, Object>> filterJson(Map<String, Object> json) {
    return json.entrySet()
               .stream()
               .filter(entry -> AvroSchemaEncoder.IS_BASE_FIELD.negate().test(entry.getKey()));
  }

  public static void verifyRecordMatchesJson(SchemaStore store, Map<String, Object> json,
    GenericRecord record) {
    LOG.debug("Comparing \nJSON: " + json + "\nRecord: " + record);
    SchemaUtil schema = new SchemaUtil(store, record);
    filterJson(json).forEach(entry -> {
      // search through each of the aliases to find a matching name in the record
      String aliasName = entry.getKey();
      String cname = schema.getCanonicalName(aliasName);
      // its an unknown field, so make sure its present
      if (cname == null) {
        RecordMetadata metadata = RecordMetadata.get(record);
        String value = metadata.getBaseFields().getUnknownFields().get(aliasName);
        assertNotNull("Didn't get an 'unknown field' value for " + aliasName, value);
        assertEquals("" + json.get(aliasName), value);
      } else {
        // ensure the value matches
        assertNotNull("Didn't find a matching canonical name for " + aliasName, cname);
        assertEquals("JSON: " + json + "\nRecord: " + record,
          entry.getValue(), record.get(cname));
      }
    });
    LOG.info("Record matches JSON!");
  }
}
