package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import io.fineo.schema.Pair;

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