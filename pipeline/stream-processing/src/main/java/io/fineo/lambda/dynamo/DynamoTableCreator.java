package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.google.common.annotations.VisibleForTesting;
import io.fineo.lambda.dynamo.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Manages creating the actual Dynamo tables, if they don't exist.
 */
public class DynamoTableCreator {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoTableCreator.class);
  private final CreateTableRequest baseRequest;
  private final DynamoTableTimeManager manager;
  private final DynamoDB dynamo;

  public DynamoTableCreator(DynamoTableTimeManager manager, DynamoDB dynamo, long readCapacity,
    long writeCapacity) {
    this.manager = manager;
    this.dynamo = dynamo;
    Pair<List<KeySchemaElement>, List<AttributeDefinition>> schema = Schema.get();
    this.baseRequest = new CreateTableRequest()
      .withKeySchema(schema.getKey())
      .withAttributeDefinitions(schema.getValue())
      .withProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity));
  }

  /**
   * Get the name of the table from the millisecond timestamp
   *
   * @param ts time of the record to map in milliseconds
   * @return name of the table created
   */
  public String getTableAndEnsureExists(long ts) {
    String name = manager.getTableName(ts);
    // The the actual heavy lifting, if the table does not exist yet
    createTable(name);
    return name;
  }

  @VisibleForTesting
  void createTable(String fullTableName) {
    // get the prefix since
    LOG.debug("Checking for table: " + fullTableName);
    if (manager.tableExists(fullTableName)) {
      return;
    }
    LOG.info("Creating table: " + fullTableName);
    baseRequest.setTableName(fullTableName);

    TableUtils.createTable(dynamo, baseRequest);
  }
}
