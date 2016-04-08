package io.fineo.lambda.dynamo.iter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.Lists;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.dynamo.ResultOrException;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoSchemaTablesResource;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@Category(AwsDependentTests.class)
public class TestScanPager {

  @ClassRule
  public static AwsDynamoResource dynamoResource = new AwsDynamoResource();
  @Rule
  public AwsDynamoSchemaTablesResource tableResource = new AwsDynamoSchemaTablesResource(dynamoResource);

  private final String primaryKey = "pk";

  @Test(expected = ResourceNotFoundException.class)
  public void testNoTablePresent() throws Exception {
    tableResource.setStoreTableCreated(false);
    read(new ScanRequest(tableResource.getTestTableName()));
  }

  @Test
  public void testReadOneRow() throws Exception {
    String name = tableResource.getTestTableName();
    Item item = new Item().withString(primaryKey, "key");
    write(createStringKeyTable(), item);
    assertResultsEqualsItems(item, read(new ScanRequest(name)));
  }

  @Test
  public void testStopRow() throws Exception {
    String name = tableResource.getTestTableName();
    Item item = new Item().withString(primaryKey, "key");
    Item item2 = new Item().withString(primaryKey, "vey");
    write(createStringKeyTable(), item, item2);
    assertResultsEqualsItems(item, read(new ScanRequest(name), "s"));
  }

  private void assertResultsEqualsItems(Item item, List<Map<String, AttributeValue>> values) {
    assertResultsEqualsItems(Lists.newArrayList(item), values);
  }

  private void assertResultsEqualsItems(List<Item> items,
    List<Map<String, AttributeValue>> values) {
    Map<String, AttributeValue> map = new HashMap<>();
    for (Item item : items) {
      for (Map.Entry<String, Object> val : item.asMap().entrySet()) {
        map.put(val.getKey(), new AttributeValue().withS((String) val.getValue()));
      }
    }
    assertEquals(Lists.newArrayList(map), values);
  }

  private Table createStringKeyTable() {
    CreateTableRequest create =
      new CreateTableRequest().withTableName(tableResource.getTestTableName())
                              .withKeySchema(new KeySchemaElement(primaryKey, KeyType.HASH))
                              .withAttributeDefinitions(
                                new AttributeDefinition(primaryKey, ScalarAttributeType.S))
                              .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
    AmazonDynamoDBAsyncClient client = tableResource.getAsyncClient();
    DynamoDB dynamo = new DynamoDB(client);
    return dynamo.createTable(create);
  }

  private void write(Table table, Item... items) {
    for (Item item : items) {
      table.putItem(item);
    }
  }

  public List<Map<String, AttributeValue>> read(ScanRequest request) {
    return read(request, null);
  }

  public List<Map<String, AttributeValue>> read(ScanRequest request, String stopKey) {
    AmazonDynamoDBAsyncClient client = tableResource.getAsyncClient();
    ScanPager pager = new ScanPager(client, request, primaryKey, stopKey);
    Iterable<ResultOrException<Map<String, AttributeValue>>> iter =
      () -> new PagingIterator<>(1, new PageManager(pager));
    return
      StreamSupport.stream(iter.spliterator(), false).map(re -> {
        re.doThrow();
        return re.getResult();
      }).collect(toList());
  }
}
