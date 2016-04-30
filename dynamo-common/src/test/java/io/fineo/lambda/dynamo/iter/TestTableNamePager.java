package io.fineo.lambda.dynamo.iter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.Lists;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class TestTableNamePager {

  @ClassRule
  public static AwsDynamoResource dynamoResource = new AwsDynamoResource();
  @Rule
  public AwsDynamoTablesResource tableResource = new AwsDynamoTablesResource(dynamoResource);

  private final String primaryKey = "pk";

  @Test
  public void testNoTables() throws Exception {
    assertEquals(newArrayList(),
      getTables(tableResource.getAsyncClient(), null, 1).collect(toList()));
  }

  @Test
  public void testReadOneTable() throws Exception {
    Table t = createStringKeyTable();
    assertEquals(newArrayList(t.getTableName()),
      getTables(tableResource.getAsyncClient(), null, 1).collect(toList()));
  }

  @Test
  public void testReadPrefix() throws Exception {
    createStringKeyTable("aname");
    String name = "bname";
    createStringKeyTable(name);

    assertEquals(newArrayList(name),
      getTables(tableResource.getAsyncClient(), "b", 1).collect(toList()));
  }

  @Test
  public void testReadFromStartKey() throws Exception {
    createStringKeyTable("name_a");
    createStringKeyTable("name_b1");
    createStringKeyTable("name_c");
    createStringKeyTable("m_name_c");

    PagingRunner<String> runner =
      new TableNamePager("name", "name_b", tableResource.getAsyncClient(), 1);
    List<String> names =
      StreamSupport.stream(new PagingIterator<>(1, new PageManager<>(newArrayList(runner)))
        .iterable().spliterator(), false).collect(toList());
    assertEquals(newArrayList("name_b1", "name_c"), names);
  }

  private Table createStringKeyTable(String tableName) {
    CreateTableRequest create =
      new CreateTableRequest().withTableName(tableName)
                              .withKeySchema(new KeySchemaElement(primaryKey, KeyType.HASH))
                              .withAttributeDefinitions(
                                new AttributeDefinition(primaryKey, ScalarAttributeType.S))
                              .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
    AmazonDynamoDBAsyncClient client = tableResource.getAsyncClient();
    DynamoDB dynamo = new DynamoDB(client);
    return dynamo.createTable(create);
  }

  private Table createStringKeyTable() {
    return createStringKeyTable(UUID.randomUUID().toString());
  }

  public static Stream<String> getTables(AmazonDynamoDBAsyncClient dynamo, String prefix, int
    pageSize) {
    PagingRunner<String> runner = new TableNamePager(prefix, dynamo, pageSize);
    return StreamSupport.stream(new PagingIterator<>(pageSize, new PageManager<>(
      newArrayList(runner))).iterable().spliterator(), false);
  }
}
