package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category(AwsDependentTests.class)
public class TestDynamoTableTimeManager {

  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);

  @Test
  public void testGetTableName() throws Exception {
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    String prefix = "table-prefix";
    DynamoTableTimeManager manager = new DynamoTableTimeManager(client, prefix);

    long oneWeek = Duration.ofDays(7).toMillis();
    String firstWeek = DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER.join(prefix, "0", oneWeek);

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
    long lastMilli = (oneDay * 7) - 1;
    System.out.println(oneDay * 7 + " vs " + firstWeek);
    assertEquals(firstWeek, manager.getTableName(lastMilli));
  }

  @Test
  public void testTableNameParsing() {
    String name = DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER.join("prefix", "0", "1");
    assertEquals(DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER.join("prefix", 0),
      DynamoTableTimeManager.getPrefixAndStart(name));
  }
}
