package io.fineo.lambda.e2e.validation.util;

import com.google.common.collect.Lists;
import io.fineo.lambda.e2e.state.EventFormTracker;
import io.fineo.lambda.util.IResourceManager;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.SchemaUtil;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.avro.TestRecordMetadata;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ValidationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationUtils.class);

  public static ByteBuffer combine(Collection<ByteBuffer> data) {
    int size = data.stream().mapToInt(bb -> bb.remaining()).sum();
    ByteBuffer combined = ByteBuffer.allocate(size);
    data.forEach(bb -> combined.put(bb));
    combined.rewind();
    return combined;
  }

  public static void empty(Function<List<ByteBuffer>, String> errorResult,
    List<ByteBuffer> records) {
    String readable = errorResult.apply(records);
    assertEquals("Found records: " + readable, Lists.newArrayList(), records);
  }

  public static Stream<Map.Entry<String, Object>> filterJson(Map<String, Object> json) {
    return json.entrySet()
               .stream()
               .filter(entry -> AvroSchemaProperties.IS_BASE_FIELD.negate().test(entry.getKey()));
  }

  public static void verifyRecordMatchesJson(SchemaStore store, Map<String, Object> json,
    GenericRecord record) {
    LOG.debug("Comparing \nJSON: " + json + "\nRecord: " + record);
    SchemaUtil schema = new SchemaUtil(store, record);
    filterJson(json).forEach(entry -> {
      // search through each of the aliases to find a matching name in the record
      String aliasName = entry.getKey();
      String cname = schema.getCanonicalFieldName(aliasName);
      // its an unknown field, so make sure its present
      if (cname == null) {
        RecordMetadata metadata = RecordMetadata.get(record);
        String value = metadata.getBaseFields().getUnknownFields().get(aliasName);
        assertNotNull("Didn't get an 'unknown field' value for " + aliasName, value);
        assertEquals("" + json.get(aliasName), value);
      } else {
        // ensure the value matches
        assertNotNull("Didn't find a matching canonical name for " + aliasName, cname);
        String eventString = "\n(expected)JSON: " + json + "\n(actual)Record: " + record;
        assertEquals("Wrong data! " + eventString,
          entry.getValue(), ((GenericData.Record) record.get(cname)).get(1));
        assertEquals("Wrong alias name! " + eventString, entry.getKey(),
          ((GenericData.Record) record.get(cname)).get(0));
      }
    });
    LOG.info("Record matches JSON!");
  }

  public static void verifyAvroRecordsFromStream(IResourceManager manager,
    EventFormTracker progress, String stream, Supplier<BlockingQueue<List<ByteBuffer>>> bytes,
    int timeout) throws IOException, InterruptedException {
    BlockingQueue<List<ByteBuffer>> queue = bytes.get();
    List<ByteBuffer> parsedBytes = new ArrayList<>();
    List<ByteBuffer> elem;
    while ((elem = queue.poll(timeout, TimeUnit.SECONDS)) != null) {
      parsedBytes.addAll(elem);
    }
    // read the parsed avro records
    List<GenericRecord> parsedRecords =
      parsedBytes.stream().map(parsed -> {
        parsed.rewind(); return parsed;
      })
                 .map(LambdaTestUtils::readRecords)
                 .flatMap(recs -> recs.stream())
                 .collect(Collectors.toList());
    verifyRecordsMatchJson(manager.getStore(), parsedRecords, progress.getExpected(),
      (record, json) -> {
        progress.setRecord(record); return null;
      }, stream);
  }

  public static void verifyRecordsMatchJson(SchemaStore store, List<GenericRecord> records,
    List<Map<String, Object>> json) {
    verifyRecordsMatchJson(store, records, json, (a, b) -> null);
  }

  public static void verifyRecordsMatchJson(SchemaStore store, List<GenericRecord> records,
    List<Map<String, Object>> json, BiFunction<GenericRecord, Map<String, Object>, ?> onMatch) {
    verifyRecordsMatchJson(store, records, json, onMatch, null);
  }

  public static void verifyRecordsMatchJson(SchemaStore store, List<GenericRecord> records,
    List<Map<String, Object>> json, BiFunction<GenericRecord, Map<String, Object>, ?> onMatch,
    String phase) {
    LOG.info("{}) Verifying records:\nExpected (json)[{}]:\n{}\nActual (recs)[{}]:{}", phase,
      json.size(), json, records.size(), records);

    for (int i = 0; i < records.size(); i++) {
      GenericRecord record = records.get(i);
      Map<String, Object> j = json.get(i);

      // org/schema naming
      TestRecordMetadata.verifyRecordMetadataMatchesExpectedNaming(record);
      verifyRecordMatchesJson(store, j, record);
      onMatch.apply(record, j);
    }
    String msg = phase == null ? "" : format("(%s) ", phase);
    assertEquals(msg + "Mismatched expected number of records and found records!", json.size(),
      records
        .size());
  }
}
