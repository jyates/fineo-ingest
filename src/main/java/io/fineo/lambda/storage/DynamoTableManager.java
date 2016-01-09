package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import javafx.util.Pair;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages the actual table creation + schema
 */
public class DynamoTableManager {

  public static final String SEPARATOR = "_";
  static final Joiner TABLE_NAME_PARTS_JOINER = Joiner.on(SEPARATOR);
  private final String prefix;
  private final LoadingCache<String, Table> cache;
  private static final ZoneId UTC = ZoneId.of("UTC");

  public DynamoTableManager(AmazonDynamoDBAsyncClient client, String prefix, long readCapacity,
    long writeCapacity) {
    this.prefix = prefix;
    Pair<List<KeySchemaElement>, List<AttributeDefinition>> schema = getSchema();
    CreateTableRequest create = new CreateTableRequest()
      .withKeySchema(schema.getKey())
      .withAttributeDefinitions(schema.getValue())
      .withProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity));
    this.cache = CacheBuilder.newBuilder()
                             .maximumSize(10)
                             .removalListener(new CloseTableRemovalListener())
                             .build(new TableLoader(client, create));
  }

  private Pair<List<KeySchemaElement>, List<AttributeDefinition>> getSchema() {
    List<KeySchemaElement> schema = new ArrayList<>();
    ArrayList<AttributeDefinition> attributes = new ArrayList<>();
    // Partition key
    schema.add(new KeySchemaElement()
      .withAttributeName(AvroToDynamoWriter.PARTITION_KEY_NAME)
      .withKeyType(KeyType.HASH));
    attributes.add(new AttributeDefinition()
      .withAttributeName(AvroToDynamoWriter.PARTITION_KEY_NAME)
      .withAttributeType(ScalarAttributeType.S));

    // sort key
    schema.add(new KeySchemaElement()
      .withAttributeName(AvroToDynamoWriter.SORT_KEY_NAME)
      .withKeyType(KeyType.RANGE));
    attributes.add(new AttributeDefinition()
      .withAttributeName(AvroToDynamoWriter.PARTITION_KEY_NAME)
      .withAttributeType(ScalarAttributeType.N));

    return new Pair<>(schema, attributes);

  }

  /**
   * Get the name of the table from the millisecond timestamp
   *
   * @param ts time of the record to map in milliseconds
   * @return
   */
  public String getTableName(long ts) {
    // map this time to the start of the week
    Instant iTs = Instant.ofEpochMilli(ts);
    LocalDateTime time = LocalDateTime.ofInstant(iTs, UTC).truncatedTo(ChronoUnit.DAYS);
    int day = time.getDayOfYear();
    // day of the year starts at 1, not 0, so we adjust to offset to make mod work nice
    int weekOffset = (day - 1) % 7;

    LocalDateTime tableStart, tableEnd;
      tableStart = time.minusDays(weekOffset);
      tableEnd = time.plusDays(7 - weekOffset);

    long start = tableStart.toInstant(ZoneOffset.UTC).toEpochMilli();
    long end = tableEnd.toInstant(ZoneOffset.UTC).toEpochMilli();
    // build the table name
    return TABLE_NAME_PARTS_JOINER.join(prefix, start, end);
  }
}