package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoSchemaTablesResource;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

@Category(AwsDependentTests.class)
public class TestDynamoTableManager {

  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();
  @Rule
  public AwsDynamoSchemaTablesResource tables = new AwsDynamoSchemaTablesResource(dynamo, false);

  @Test
  public void testCreateTable() throws Exception {
    AmazonDynamoDBAsyncClient client = tables.getAsyncClient();

    DynamoTableCreator loader =
      new DynamoTableCreator(new DynamoTableManager(client, UUID.randomUUID().toString()),
        new DynamoDB(client), 10, 10);
    String prefix = tables.getClientProperties().getDynamoIngestTablePrefix();
    String name = DynamoTableManager.TABLE_NAME_PARTS_JOINER.join(prefix, "1", "2");
    loader.createTable(name);
    oneTableWithPrefix(client, prefix);

    loader.createTable(name);
    oneTableWithPrefix(client, prefix);

    // something happened and the table gets deleted
    client.deleteTable(name);
    loader.createTable(name);
    oneTableWithPrefix(client, prefix);

    // another table gets created with a different range, ensure that we still don't try and create
    // the table again
    String earlierTableName = DynamoTableManager.TABLE_NAME_PARTS_JOINER.join(prefix, "0", "1");
    loader.createTable(earlierTableName);
    loader.createTable(name);
  }

  private void oneTableWithPrefix(AmazonDynamoDBAsyncClient client, String prefix) {
    assertEquals(1, client.listTables(prefix, 10).getTableNames().size());
  }
}
