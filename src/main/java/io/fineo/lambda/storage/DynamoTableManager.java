package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
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
  private static final ZoneId UTC = ZoneId.of("UTC");
  private final CreateTableRequest baseRequest;
  private final AmazonDynamoDBAsyncClient client;
  private final DynamoDB dynamo;

  public DynamoTableManager(AmazonDynamoDBAsyncClient client, String prefix, long readCapacity,
    long writeCapacity) {
    this.prefix = prefix;
    Pair<List<KeySchemaElement>, List<AttributeDefinition>> schema = getSchema();
    this.baseRequest = new CreateTableRequest()
      .withKeySchema(schema.getKey())
      .withAttributeDefinitions(schema.getValue())
      .withProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity));
    this.client = client;
    this.dynamo = new DynamoDB(client);
  }

  /**
   * Split the full name into its prefix and start time, essentially the non-inlcusive prefix of
   * the table.
   * @param fullTableName full name of the table, in the format described in the
   * {@link DynamoTableManager}
   * @return the non-inlcusive prefix of the table table to search in AWS
   */
  @VisibleForTesting
  static String getPrefixAndStart(String fullTableName) {
    return fullTableName.substring(0, fullTableName.lastIndexOf(SEPARATOR));
  }

  /**
   * Get the name of the table from the millisecond timestamp
   *
   * @param ts time of the record to map in milliseconds
   * @return
   */
  public String getTableAndEnsureExists(long ts) {
    String name = getTableName(ts);
    // The the actual heavy lifting, if the table does not exist yet
    createTable(name);
    return name;
  }

  @VisibleForTesting
  String getTableName(long ts){
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

  @VisibleForTesting
  void createTable(String fullTableName) {
    // get the prefix since
    String tableAndStart = DynamoTableManager.getPrefixAndStart(fullTableName);
    ListTablesResult result = client.listTables(tableAndStart, 1);
    // table exists, get a reference to it
    if(result.getTableNames().size() > 0){
      return;
    }
    baseRequest.setTableName(fullTableName);
    dynamo.createTable(baseRequest);
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
      .withAttributeName(AvroToDynamoWriter.SORT_KEY_NAME)
      .withAttributeType(ScalarAttributeType.N));

    return new Pair<>(schema, attributes);
  }
}