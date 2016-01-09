package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestDyanmoTableManager {

  @Test
  public void testGetTableName() throws Exception {
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    String prefix = "table-prefix";
    DynamoTableManager manager = new DynamoTableManager(client, prefix, 10, 10);

    long oneWeek = Duration.ofDays(7).toMillis();
    String firstWeek = DynamoTableManager.TABLE_NAME_PARTS_JOINER.join(prefix, "0", oneWeek);

    assertEquals(firstWeek, manager.getTableName(0));
    assertEquals(firstWeek, manager.getTableName(1000));

    long oneDay = Duration.ofDays(1).toMillis();
    assertEquals(firstWeek, manager.getTableName(oneDay)); // midnight 1970-01-02
    assertEquals(firstWeek, manager.getTableName(oneDay*2));
    assertEquals(firstWeek, manager.getTableName(oneDay*3));
    assertEquals(firstWeek, manager.getTableName(oneDay*4));
    assertEquals(firstWeek, manager.getTableName(oneDay*5));
    assertEquals(firstWeek, manager.getTableName(oneDay * 6));

    // check the boundary conditions
    assertNotEquals(firstWeek, manager.getTableName(oneDay * 7));
    long lastMilli = (oneDay * 7) -1;
    System.out.println(oneDay*7+" vs "+firstWeek);
    assertEquals(firstWeek, manager.getTableName(lastMilli));
  }
}
