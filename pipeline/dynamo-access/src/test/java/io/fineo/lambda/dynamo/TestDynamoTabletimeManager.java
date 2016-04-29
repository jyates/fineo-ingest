package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category(AwsDependentTests.class)
public class TestDynamoTableTimeManager {

  private static final ZoneId UTC = ZoneId.of("UTC");
  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);

  private final String prefix = "table-prefix";
  private final long oneDay = Duration.ofDays(1).toMillis();

  @Test
  public void testGetTableName() throws Exception {
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    DynamoTableTimeManager manager = new DynamoTableTimeManager(client, prefix);

    long oneWeek = Duration.ofDays(7).toMillis();
    Instant now = Instant.now();
    LocalDateTime dt = LocalDateTime.ofInstant(now, UTC);
    String firstWeek = getTableName(0, oneWeek, now);
    assertEquals(firstWeek, manager.getTableName(now, 0));
    assertEquals(firstWeek, manager.getTableName(now, 1000));

    // check the same month, different day, as well as different month
    Instant tomorrow = now.plus(1, ChronoUnit.DAYS);
    LocalDateTime dtTomorrow = LocalDateTime.ofInstant(tomorrow, UTC);
    Instant sameMonth;
    Instant nextMonth;
    if (dtTomorrow.getMonthValue() == dt.getMonthValue()) {
      sameMonth = tomorrow;
      nextMonth = now.plus(31, ChronoUnit.DAYS);
    } else {
      nextMonth = tomorrow;
      sameMonth = now.minus(1, ChronoUnit.DAYS);
    }
    // midnight 1970-01-02
    assertTableNamesForWeek(manager, firstWeek, sameMonth);
    assertTableNamesForWeek(manager, getTableName(0, oneWeek, nextMonth), nextMonth);

    // check the boundary conditions
    assertNotEquals(firstWeek, manager.getTableName(now, oneDay * 7));
    long lastMilli = (oneDay * 7) - 1;
    assertEquals(firstWeek, manager.getTableName(now, lastMilli));
  }

  @Test
  public void testCoveringTables() throws Exception{
    AmazonDynamoDBAsyncClient client = tables.getAsyncClient();
    DynamoTableTimeManager manager = new DynamoTableTimeManager(client, prefix);
    // create a series of tables, across different time ranges and write dates
    // fwt - months 2015 -11,12; 2016 -1,2,3
    // cwt - weeks 1, 2, 3, 4, 5
    // divided as m1 - w1, m2 - w1,w2, m3 - w1,w2,w3, m4 - w1,w2,w3,w4, m5 - w1,w2,w3,w4.w5
    LocalDateTime.of(2014, 1, 1, 1);

    // covering cwt tables of 0-2, 2-4, 3-5, which will span all the appropriate FWT tables
  }

  private CreateTableRequest getCreateTableRequest() {
    return new CreateTableRequest()
      .withKeySchema(new KeySchemaElement("key", KeyType.HASH))
      .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
  }

  private String getTableName(long writeStart, long writeEnd, Instant monthYear) {
    LocalDateTime dt = LocalDateTime.ofInstant(monthYear, UTC);
    return DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER
      .join(prefix, writeStart, writeEnd, dt.getMonthValue(), dt.getYear());
  }

  private void assertTableNamesForWeek(DynamoTableTimeManager manager, String tableName,
    Instant fineoWriteTime) {
    assertTableNamesForWeek(manager, tableName, fineoWriteTime, oneDay);
  }

  private void assertTableNamesForWeek(DynamoTableTimeManager manager, String tableName,
    Instant fineoWriteTime, long clientWriteTime) {
    assertEquals(tableName, manager.getTableName(fineoWriteTime, clientWriteTime));
    assertEquals(tableName, manager.getTableName(fineoWriteTime, clientWriteTime * 2));
    assertEquals(tableName, manager.getTableName(fineoWriteTime, clientWriteTime * 3));
    assertEquals(tableName, manager.getTableName(fineoWriteTime, clientWriteTime * 4));
    assertEquals(tableName, manager.getTableName(fineoWriteTime, clientWriteTime * 5));
    assertEquals(tableName, manager.getTableName(fineoWriteTime, clientWriteTime * 6));

  }

  @Test
  public void testTableNameParsing() {
    String name = DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER.join("prefix", "0", "1");
    assertEquals(DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER.join("prefix", 0),
      DynamoTableTimeManager.getPrefixAndStart(name));
  }
}
