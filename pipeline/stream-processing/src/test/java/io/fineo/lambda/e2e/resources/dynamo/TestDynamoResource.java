package io.fineo.lambda.e2e.resources.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.dynamo.DynamoTableManager;
import io.fineo.lambda.dynamo.avro.Schema;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoSchemaTablesResource;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

@Category(AwsDependentTests.class)
public class TestDynamoResource {

  private static final Log LOG = LogFactory.getLog(TestDynamoResource.class);

  @ClassRule
  public static AwsDynamoResource dynamoResource = new AwsDynamoResource();
  @Rule
  public AwsDynamoSchemaTablesResource tableResource =
    new AwsDynamoSchemaTablesResource(dynamoResource);
  @ClassRule
  public static TestOutput output = new TestOutput(false);

  @Test
  public void testWriteAndCopyToFile() throws Exception {
    Properties props = new Properties();
    String prefix = "test-ingest";
    props.put(LambdaClientProperties.TEST_PREFIX, prefix);
    props.put(LambdaClientProperties.DYNAMO_INGEST_TABLE_PREFIX, prefix);
    props.put(LambdaClientProperties.DYNAMO_SCHEMA_STORE_TABLE, tableResource.getTestTableName());
    LambdaClientProperties clientProperties = tableResource.getClientProperties(props);

    AmazonDynamoDBAsyncClient client = clientProperties.getDynamo();
    DynamoResource dynamo = new DynamoResource(client, new ResultWaiter
      .ResultWaiterFactory(1000, 100), () -> clientProperties.createSchemaStore(),
      clientProperties);
    ListeningExecutorService exec =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());

    // have to setup the schema store because the resource expects a schema table or no other tables
    FutureWaiter waiter = new FutureWaiter(exec);
    dynamo.setup(waiter);
    waiter.await();

    DynamoTableManager tables =
      new DynamoTableManager(client, clientProperties.getDynamoIngestTablePrefix());
    DynamoTableManager.DynamoTableCreator creator = tables.creator(1, 1);
    String name = creator.getTableAndEnsureExists(System.currentTimeMillis());

    Map<String, AttributeValue> item = new HashMap<>();
    item.put(Schema.PARTITION_KEY_NAME, new AttributeValue("partition"));
    item.put(Schema.SORT_KEY_NAME, new AttributeValue().withN("1"));
    client.putItem(name, item);

    // now write the data to a file
    File out = output.newFolder("dynamo");
    dynamo.copyStoreTables(out);
    LOG.info("data is at: " + out);
  }
}
