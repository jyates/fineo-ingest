package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.cache.CacheLoader;

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

  @Override
  public Table load(String key) throws Exception {
    String tableAndStart = DyanmoTableManager.getPrefixAndStart(key);
    ListTablesResult result = client.listTables(tableAndStart, 1);
    // table exists, get a reference to it
    if(result.getTableNames().size() > 0){
      return dynamo.getTable(key);
    }
    baseRequest.setTableName(key);
    return dynamo.createTable(baseRequest);
  }
}
