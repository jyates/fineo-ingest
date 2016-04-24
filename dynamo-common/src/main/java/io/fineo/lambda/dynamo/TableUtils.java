package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.Lists;
import io.fineo.lambda.dynamo.iter.PageManager;
import io.fineo.lambda.dynamo.iter.PagingIterator;
import io.fineo.lambda.dynamo.iter.PagingRunner;
import io.fineo.lambda.dynamo.iter.TableNamePager;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 */
public class TableUtils {
  public static void createTable(DynamoDB dynamo, CreateTableRequest baseRequest) {
    Table t = dynamo.createTable(baseRequest);
    while (true) {
      try {
        TableDescription desc = t.waitForActiveOrDelete();
        if (desc == null) {
          throw new RuntimeException(
            "Table " + baseRequest.getTableName() + " was deleted while waiting for it to become active!");
        }
        break;
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  public static Stream<String> getTables(AmazonDynamoDBAsyncClient dynamo, String prefix, int
    pageSize) {
    PagingRunner<String> runner = new TableNamePager(prefix, dynamo, pageSize);
    return StreamSupport.stream(new PagingIterator<>(pageSize, new PageManager<>(
      Lists.newArrayList(runner))).iterable().spliterator(), false);
  }
}
