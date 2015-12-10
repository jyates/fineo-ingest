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
import com.google.common.collect.Lists;
import io.fineo.internal.customer.BaseFields;
import io.fineo.schema.avro.AvroRecordDecoder;
import javafx.util.Pair;
import org.apache.avro.generic.GenericRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Manages the actual table creation + schema
 */
public class DyanmoTableManager {

  public static final String SEPARATOR = "_";
  private static final Joiner JOIN = Joiner.on(SEPARATOR);
  private final String prefix;
  private final LoadingCache<String, Table> cache;

  public DyanmoTableManager(AmazonDynamoDBAsyncClient client, String prefix, long readCapacity,
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

  public static String getPrefixAndStart(String fullTableName) {
    String[] parts = fullTableName.split(SEPARATOR);
    return JOIN.join(Lists.newArrayList(parts).subList(0, parts.length - 1));
  }

  public Table getTable(long ts) throws ExecutionException {
    return cache.get(getTableName(ts));
  }

  public String getTableName(long ts) {
    // map this time to the start of the week
    Instant time = Instant.ofEpochMilli(ts);
    LocalDateTime date = LocalDateTime.from(time);
    LocalDateTime tableStart = date.truncatedTo(ChronoUnit.WEEKS);
    LocalDateTime tableEnd = tableStart.plus(1, ChronoUnit.WEEKS);

    long start = tableStart.toEpochSecond(ZoneOffset.UTC);
    long end = tableEnd.toEpochSecond(ZoneOffset.UTC);
    // build the table name
    return JOIN.join(prefix, start, end);
  }
}