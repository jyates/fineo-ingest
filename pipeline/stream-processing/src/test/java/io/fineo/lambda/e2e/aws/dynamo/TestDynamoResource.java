package io.fineo.lambda.e2e.aws.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Guice;
import com.google.inject.Provider;
import io.fineo.aws.AwsDependentTests;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.SchemaStoreModule;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoSchemaTablesResource;
import io.fineo.lambda.e2e.manager.collector.FileCollector;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.store.SchemaStore;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;

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
    props.put(FineoProperties.TEST_PREFIX, prefix);
    props.put(FineoProperties.DYNAMO_INGEST_TABLE_PREFIX, prefix);
    props.put(FineoProperties.DYNAMO_SCHEMA_STORE_TABLE, tableResource.getTestTableName());
    LambdaClientProperties clientProperties = tableResource.getClientProperties(props);

    AmazonDynamoDBAsyncClient client = tableResource.getAsyncClient();
    Provider<SchemaStore> store = Guice.createInjector(
      new SchemaStoreModule(),
      new PropertiesModule(clientProperties.getRawPropertiesForTesting()),
      instanceModule(client)).getProvider(SchemaStore.class);
    DynamoResource dynamo = new DynamoResource(client, new ResultWaiter
      .ResultWaiterFactory(1000, 100), store,
      clientProperties);
    ListeningExecutorService exec =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());

    // have to setup the schema store because the resource expects a schema table or no other tables
    FutureWaiter waiter = new FutureWaiter(exec);
    dynamo.setup(waiter);
    waiter.await();

    DynamoTableTimeManager tables =
      new DynamoTableTimeManager(client, clientProperties.getDynamoIngestTablePrefix());
    DynamoTableCreator creator = new DynamoTableCreator(tables, new DynamoDB(client), 1, 1);
    String name = creator.getTableAndEnsureExists(System.currentTimeMillis());

    Map<String, AttributeValue> item = new HashMap<>();
    item.put(Schema.PARTITION_KEY_NAME, new AttributeValue("partition"));
    item.put(Schema.SORT_KEY_NAME, new AttributeValue().withN("1"));
    client.putItem(name, item);

    // now write the data to a file
    FileCollector collector = new FileCollector(output);
    dynamo.copyStoreTables(collector.getNextLayer("dynamo"));
    LOG.info("data is at: " + collector.getRoot());
  }
}
