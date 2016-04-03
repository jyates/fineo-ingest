package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.fineo.aws.AwsDependentTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestDyanmoTableManager {

  @Test
  public void testGetTableName() throws Exception {
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    String prefix = "table-prefix";
    DynamoTableManager manager = new DynamoTableManager(client, prefix);

    long oneWeek = Duration.ofDays(7).toMillis();
    String firstWeek = DynamoTableManager.TABLE_NAME_PARTS_JOINER.join(prefix, "0", oneWeek);

    assertEquals(firstWeek, manager.getTableName(0));
    assertEquals(firstWeek, manager.getTableName(1000));

    long oneDay = Duration.ofDays(1).toMillis();
    assertEquals(firstWeek, manager.getTableName(oneDay)); // midnight 1970-01-02
    assertEquals(firstWeek, manager.getTableName(oneDay * 2));
    assertEquals(firstWeek, manager.getTableName(oneDay * 3));
    assertEquals(firstWeek, manager.getTableName(oneDay * 4));
    assertEquals(firstWeek, manager.getTableName(oneDay * 5));
    assertEquals(firstWeek, manager.getTableName(oneDay * 6));

    // check the boundary conditions
    assertNotEquals(firstWeek, manager.getTableName(oneDay * 7));
    long lastMilli = (oneDay * 7) -1;
    System.out.println(oneDay*7+" vs "+firstWeek);
    assertEquals(firstWeek, manager.getTableName(lastMilli));
  }

  @Test
  public void testTableNameParsing() {
    String name = DynamoTableManager.TABLE_NAME_PARTS_JOINER.join("prefix", "0", "1");
    assertEquals(DynamoTableManager.TABLE_NAME_PARTS_JOINER.join("prefix", 0),
      DynamoTableManager.getPrefixAndStart(name));
  }

  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();

  @Test
  @Category(AwsDependentTests.class)
  public void testCreateTable() throws Exception {
    AmazonDynamoDBAsyncClient client = dynamo.getAsyncClient();

    DynamoTableManager.DynamoTableWriter loader =
      new DynamoTableManager(client, UUID.randomUUID().toString()).writer(10, 10);
    String prefix = "prefix";
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
    assertEquals(1, client.listTables("prefix", 10).getTableNames().size());
  }
}
