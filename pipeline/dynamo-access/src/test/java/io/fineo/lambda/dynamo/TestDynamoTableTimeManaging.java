package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category(AwsDependentTests.class)
public class TestDynamoTableTimeManaging {

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
    Instant tomorrow = now.plus(1, DAYS);
    LocalDateTime dtTomorrow = LocalDateTime.ofInstant(tomorrow, UTC);
    Instant sameMonth;
    Instant nextMonth;
    if (dtTomorrow.getMonthValue() == dt.getMonthValue()) {
      sameMonth = tomorrow;
      nextMonth = now.plus(31, DAYS);
    } else {
      nextMonth = tomorrow;
      sameMonth = now.minus(1, DAYS);
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
  public void testGetTableNameMonthBoundaries() throws Exception {
    Instant fwt = inst(LocalDateTime.of(2015, 11, 1, 1, 1));
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    DynamoTableTimeManager manager = new DynamoTableTimeManager(client, prefix);
    String name = manager.getTableName(fwt, inst(
      LocalDateTime.of(2015, 11, 2, 1, 1)).toEpochMilli());
    String name2 =
      manager.getTableName(fwt, inst(LocalDateTime.of(2015, 11, 4, 1, 1)).toEpochMilli());
    assertEquals(
      "Days in different months, and should have the same 'week in year', but don't! " +
      "\nSource: " + getHumanReadableTableName(name) +
      "\nTarget: " + getHumanReadableTableName(name2),
      name, name2);
  }

  private String getHumanReadableTableName(String name) {
    String[] parts = name.split(DynamoTableTimeManager.SEPARATOR);
    Instant start = Instant.ofEpochMilli(Long.parseLong(parts[1]));
    Instant end = Instant.ofEpochMilli(Long.parseLong(parts[2]));
    parts[1] = "[" + start + "]";
    parts[2] = "[" + end + "]";
    return Joiner.on(DynamoTableTimeManager.SEPARATOR).join(parts);
  }

  @Test
  public void testCoveringTables() throws Exception {
    AmazonDynamoDBAsyncClient client = tables.getAsyncClient();
    DynamoTableTimeManager manager = new DynamoTableTimeManager(client, prefix);
    // create a series of tables, across different time ranges and write dates
    // fwt - months 2015 -11,12; 2016 -1,2,3
    List<Instant> fwt = asList(
      inst(LocalDateTime.of(2015, 11, 1, 1, 1)),
      inst(LocalDateTime.of(2015, 12, 1, 1, 1)),
      inst(LocalDateTime.of(2016, 1, 1, 1, 1)),
      inst(LocalDateTime.of(2016, 2, 1, 1, 1)),
      inst(LocalDateTime.of(2016, 3, 1, 1, 1)));

    List<Instant> cwt = asList(
      inst(LocalDateTime.of(2015, 10, 1, 1, 1)),
      inst(LocalDateTime.of(2015, 11, 8, 1, 1)),
      inst(LocalDateTime.of(2015, 11, 16, 1, 1)),
      inst(LocalDateTime.of(2015, 11, 24, 1, 1)),
      inst(LocalDateTime.of(2015, 12, 4, 1, 1)));

    DynamoDB dynamo = new DynamoDB(client);
    // cwt - weeks 1, 2, 3, 4, 5
    // divided as m1 - w1, m2 - w1,w2, m3 - w1,w2,w3, m4 - w1,w2,w3,w4, m5 - w1,w2,w3,w4.w5
    Multimap<Instant, String> tableNames = ArrayListMultimap.create();
    for (int i = 0; i < 5; i++) {
      Instant writeTime = fwt.get(i);
      for (int j = 0; j <= i; j++) {
        Instant customerTime = cwt.get(j);
        // create the table
        String name = manager.getTableName(writeTime, customerTime.toEpochMilli());
        tableNames.put(customerTime, name);
        CreateTableRequest create = getCreateTableRequest().withTableName(name);
        TableUtils.createTable(dynamo, create);
      }
    }

    // covering cwt tables of 0-1, 0-2, 2-4, 3-5, which will span all the appropriate FWT tables
    Instant start = cwt.get(0);
    Instant end = start.plus(5, DAYS);
    List<String> expectedTables = getTableNames(tableNames, start);
    assertEquals(expectedTables,
      getTableNames(manager.getCoveringTableNames(new Range<>(start, end))));

    end = cwt.get(1);
    expectedTables = getTableNames(tableNames, start, end);
    assertEquals(expectedTables,
      getTableNames(manager.getCoveringTableNames(new Range<>(start, end))));
    assertEquals("Same table names for non-original write time, but still inside week",
      expectedTables,
      getTableNames(manager.getCoveringTableNames(new Range<>(start, end.plus(1, DAYS)))));

    start = cwt.get(1);
    end = cwt.get(3);
    expectedTables = getTableNames(tableNames, start, cwt.get(2), end);
    assertEquals(expectedTables,
      getTableNames(manager.getCoveringTableNames(new Range<>(start, end))));
  }

  @Test
  public void testParseTableParts() throws Exception {
    Instant fwt = inst(LocalDateTime.of(2015, 11, 1, 1, 1));
    AmazonDynamoDBAsyncClient client = Mockito.mock(AmazonDynamoDBAsyncClient.class);
    DynamoTableTimeManager manager = new DynamoTableTimeManager(client, prefix);
    long epoch = inst(LocalDateTime.of(2015, 11, 2, 1, 1)).toEpochMilli();
    String name = manager.getTableName(fwt, epoch);
    DynamoTableNameParts parts = new DynamoTableNameParts(prefix, 1446076800000l, 1446681600000l,
      11, 2015);
    assertEquals(parts, DynamoTableNameParts.parse(prefix, name));

    // try another table that _should_ end up with the same table
    name = manager.getTableName(fwt, inst(LocalDateTime.of(2015, 11, 4, 1, 1)).toEpochMilli());
    assertEquals(parts, DynamoTableNameParts.parse(prefix, name));

    assertEquals(parts, DynamoTableNameParts.parse(name, true));
    DynamoTableNameParts parsed = DynamoTableNameParts.parse(name, false);
    // check each of the sub-parts
    assertEquals(parts.getStart(), parsed.getStart());
    assertEquals(parts.getEnd(), parsed.getEnd());
    assertEquals(parts.getWriteMonth(), parsed.getWriteMonth());
    assertEquals(parts.getWriteYear(), parsed.getWriteYear());
  }

  @Test
  public void testCreateTable() throws Exception {
    AmazonDynamoDBAsyncClient client = tables.getAsyncClient();

    DynamoTableCreator loader =
      new DynamoTableCreator(new DynamoTableTimeManager(client, UUID.randomUUID().toString()),
        new DynamoDB(client), 10, 10);
    String name = DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER.join(prefix, "1", "2");
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
    String earlierTableName = DynamoTableTimeManager.TABLE_NAME_PARTS_JOINER.join(prefix, "0", "1");
    loader.createTable(earlierTableName);
    loader.createTable(name);
  }

  @Test
  public void testTableNamesWithEarlySeparator() throws Exception {
    AmazonDynamoDBAsyncClient client = tables.getAsyncClient();
    DynamoTableTimeManager time = new DynamoTableTimeManager(client, "some" + DynamoTableTimeManager
      .SEPARATOR + "table");
    DynamoTableCreator tables = new DynamoTableCreator(time, new DynamoDB(client), 10, 10);
    String name = tables.getTableAndEnsureExists(100);
    assertEquals(name, time.getCoveringTableNames(new Range<>(Instant.ofEpochMilli(0), Instant
      .ofEpochMilli(101))).stream().map(p -> p.getKey()).findFirst().get());
  }

  private void oneTableWithPrefix(AmazonDynamoDBAsyncClient client, String prefix) {
    assertEquals(1, client.listTables(prefix, 10).getTableNames().size());
  }

  private List<String> getTableNames(Multimap<Instant, String> tableMap, Instant... times) {
    List<String> names = new ArrayList<>();
    for (Instant time : times) {
      names.addAll(tableMap.get(time));
    }
    Collections.sort(names);
    return names;
  }

  private List<String> getTableNames(List<Pair<String, Range<Instant>>> actualTables) {
    return actualTables.stream().map(pair -> pair.getKey()).collect(toList());
  }

  private static Instant inst(LocalDateTime time) {
    return time.toInstant(ZoneOffset.UTC);
  }

  private CreateTableRequest getCreateTableRequest() {
    return new CreateTableRequest()
      .withKeySchema(new KeySchemaElement("key", KeyType.HASH))
      .withAttributeDefinitions(
        new AttributeDefinition().withAttributeName("key").withAttributeType(ScalarAttributeType.S))
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
