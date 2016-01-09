package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;

/**
 * Create a {@link Table} on request
 */
public class TableLoader extends CacheLoader<String, Table> {

  private final AmazonDynamoDBAsyncClient client;
  private final DynamoDB dynamo;
  private final CreateTableRequest baseRequest;

  public TableLoader(AmazonDynamoDBAsyncClient client, CreateTableRequest baseRequest){
    this.client = client;
    this.dynamo = new DynamoDB(client);
    this.baseRequest = baseRequest;
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
    return fullTableName.substring(0, fullTableName.lastIndexOf(DynamoTableManager.SEPARATOR));
  }

  /**
   * Load a table with the given table name. Checks to see if the table has already been created
   * it and, if it hasn't, creates the table.
   * @param fullTableName full name of the table, in the format described in the
   * {@link DynamoTableManager}
   * @return a connection toa  table
   * @throws Exception if the table could not be created or reached
   */
  @Override
  public Table load(String fullTableName) throws Exception {
    // get the prefix since
    String tableAndStart = getPrefixAndStart(fullTableName);
    ListTablesResult result = client.listTables(tableAndStart, 1);
    // table exists, get a reference to it
    if(result.getTableNames().size() > 0){
      return dynamo.getTable(fullTableName);
    }
    baseRequest.setTableName(fullTableName);
    return dynamo.createTable(baseRequest);
  }
}
