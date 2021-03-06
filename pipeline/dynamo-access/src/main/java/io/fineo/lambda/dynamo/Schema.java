package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

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
  public static final String PARTITION_KEY_NAME = "_foid_mid";
  /**
   * shortened for 'timestamp'
   */
  public static final String SORT_KEY_NAME = "_fts";

  /**
   * Time that the record was written
   */
  public static final String WRITE_TIME_FIELD = "_fwt";

  /**
   * Original alias for the metric that was written
   */
  public static final String METRIC_ORIGINAL_ALIAS_FIELD = "_foa";

  /**
   * Id field used to collect the ids used to handle overlapping events at the same time
   */
  public static final String ID_FIELD = "_fid";

  /**
   * Exposed so the readerator has a simple map of excluded fields
   */
  public static List<String> INTERNAL_FIELDS = newArrayList(
    PARTITION_KEY_NAME,
    SORT_KEY_NAME,
    WRITE_TIME_FIELD,
    METRIC_ORIGINAL_ALIAS_FIELD,
    ID_FIELD
  );

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

    SCHEMA = new ImmutablePair<>(schema, attributes);
  }

  public static Pair<List<KeySchemaElement>, List<AttributeDefinition>> get() {
    return SCHEMA;
  }

  public static AttributeValue getSortKey(Long ts) {
    return new AttributeValue().withN(ts.toString());
  }

  public static AttributeValue getPartitionKey(String orgID, String metricCanonicalName) {
    return new AttributeValue(orgID + metricCanonicalName);
  }
}
