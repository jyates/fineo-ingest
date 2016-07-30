package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ListTablesSpec;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import java.util.Iterator;

/**
 *
 */
public class TableUtils {
  public static Table createTable(DynamoDB dynamo, CreateTableRequest baseRequest) {
    Table t = dynamo.createTable(baseRequest);
    while (true) {
      try {
        TableDescription desc = t.waitForActiveOrDelete();
        if (desc == null) {
          throw new RuntimeException(
            "Table " + baseRequest
              .getTableName() + " was deleted while waiting for it to become active!");
        }
        return t;
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  public static Iterator<String> getTables(AmazonDynamoDBAsyncClient client, String tableAndStart,
    int pageSize) {
    return getTables(client, tableAndStart, tableAndStart, pageSize);
  }

  public static Iterator<String> getTables(AmazonDynamoDBAsyncClient client, String tableAndStart,
    String prefix, int pageSize) {
    DynamoDB dynamo = new DynamoDB(client);
    ListTablesSpec spec = new ListTablesSpec();
    spec.withMaxPageSize(pageSize);
    spec.withExclusiveStartTableName(tableAndStart);
    Iterator<Table> results = Iterators.whereStop(dynamo.listTables(spec).iterator(),
      table -> table.getTableName().startsWith(prefix));
    return com.google.common.collect.Iterators.transform(results, Table::getTableName);
  }
}
