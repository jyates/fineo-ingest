package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import io.fineo.lambda.util.EndToEndTestRunner;
import io.fineo.lambda.util.SchemaUtil;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.AvroRecordDecoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DynamoTestUtils {

  public static void validateDynamoRecord(Item item,
    Stream<Pair<String, Object>> expectedAttributes, Function<String, String> schema) {
    Map<String, Object> attributes = item.asMap();
    expectedAttributes.forEach(entry -> {
      String cname = schema.apply(entry.getKey());
      assertNotNull("No cannonical name for alias field: " + entry.getKey(), cname);
      assertEquals(entry.getValue(), attributes.get(cname));
    });
  }
}