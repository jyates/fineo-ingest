package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import io.fineo.aws.AwsDependentTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestTableLoader {

  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();

  @Test
  public void testTableNameParsing() {
    String name = DynamoTableManager.TABLE_NAME_PARTS_JOINER.join("prefix", "0", "1");
    assertEquals(DynamoTableManager.TABLE_NAME_PARTS_JOINER.join("prefix", 0),
      TableLoader.getPrefixAndStart(name));
  }

  @Test
  @Category(AwsDependentTests.class)
  public void testCreateTable() throws Exception {
    dynamo.start();
    AmazonDynamoDBAsyncClient client = dynamo.getAsyncClient();
    CreateTableRequest base = createRequestBase();

    TableLoader loader = new TableLoader(client, base);
    String prefix = "prefix";
    String name = DynamoTableManager.TABLE_NAME_PARTS_JOINER.join(prefix, "1", "2");
    loader.load(name);
    oneTableWithPrefix(client, prefix);

    loader.load(name);
    oneTableWithPrefix(client, prefix);

    // something happened and the table gets deleted
    client.deleteTable(name);
    loader.load(name);
    oneTableWithPrefix(client, prefix);

    // another table gets created with a different range, ensure that we still don't try and create
    // the table again
    String earlierTableName = DynamoTableManager.TABLE_NAME_PARTS_JOINER.join(prefix, "0", "1");
    loader.load(earlierTableName);
    loader.load(name);
  }

  private void oneTableWithPrefix(AmazonDynamoDBAsyncClient client, String prefix) {
    assertEquals(1, client.listTables("prefix", 10).getTableNames().size());
  }

  private CreateTableRequest createRequestBase() {
    List<KeySchemaElement> schema = new ArrayList<>();
    ArrayList<AttributeDefinition> attributes = new ArrayList<>();
    // Partition key
    schema.add(new KeySchemaElement()
      .withAttributeName(AvroToDynamoWriter.PARTITION_KEY_NAME)
      .withKeyType(KeyType.HASH));
    attributes.add(new AttributeDefinition()
      .withAttributeName(AvroToDynamoWriter.PARTITION_KEY_NAME)
      .withAttributeType(ScalarAttributeType.S));

    CreateTableRequest create = new CreateTableRequest()
      .withKeySchema(schema)
      .withAttributeDefinitions(attributes)
      .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L));

    return create;
  }
}