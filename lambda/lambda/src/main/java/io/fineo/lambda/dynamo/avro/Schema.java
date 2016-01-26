package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import io.fineo.schema.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper methods for translation to/from Avro and the Dynamo table schema
 * <p>
 * The schema of the table is as follows:
 * <table>
 * <tr><th>Partition Key</th><th>Sort Key</th><th>Known Fields</th><th>Unknown Fields</th></tr>
 * <tr><td>[orgID]_[metricId]</td><td>[timestamp]</td><td>encoded as
 * type</td><td>string
 * encoded</td></tr>
 * </table>
 * </p>
 */
public class Schema {
  /**
   * name of the column shortened (for speed) of "org ID" and "metric id"
   */
  public static final String PARTITION_KEY_NAME = "oid_mid";
  /**
   * shortened for 'timestamp'
   */
  public static final String SORT_KEY_NAME = "ts";
  private static final Pair<List<KeySchemaElement>, List<AttributeDefinition>> SCHEMA;

  static {
    List<KeySchemaElement> schema = new ArrayList<>();
    ArrayList<AttributeDefinition> attributes = new ArrayList<>();
    // Partition key
    schema.add(new KeySchemaElement()
      .withAttributeName(PARTITION_KEY_NAME)
      .withKeyType(KeyType.HASH));
    attributes.add(new AttributeDefinition()
      .withAttributeName(PARTITION_KEY_NAME)
      .withAttributeType(ScalarAttributeType.S));

    // sort key
    schema.add(new KeySchemaElement()
      .withAttributeName(SORT_KEY_NAME)
      .withKeyType(KeyType.RANGE));
    attributes.add(new AttributeDefinition()
      .withAttributeName(SORT_KEY_NAME)
      .withAttributeType(ScalarAttributeType.N));

    SCHEMA = new Pair<>(schema, attributes);
  }

  public static Pair<List<KeySchemaElement>, List<AttributeDefinition>> get() {
    return SCHEMA;
  }

  static AttributeValue convertField(org.apache.avro.Schema.Field field, Object value) {
    org.apache.avro.Schema.Type type = field.schema().getType();
    switch (type) {
      case STRING:
        return new AttributeValue(String.valueOf(value));
      case BYTES:
        return new AttributeValue().withB(ByteBuffer.wrap((byte[]) value));
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new AttributeValue().withN(value.toString());
      case BOOLEAN:
        return new AttributeValue().withBOOL(Boolean.valueOf(value.toString()));
      default:
        return null;
    }
  }

  static AttributeValue getSortKey(Long ts) {
    return new AttributeValue().withN(ts.toString());
  }

  static AttributeValue getPartitionKey(String orgID, String metricCanonicalName) {
    return new AttributeValue(orgID + metricCanonicalName);
  }
}
