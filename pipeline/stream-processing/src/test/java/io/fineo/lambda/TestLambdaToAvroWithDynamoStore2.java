package io.fineo.lambda;

import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Provider;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.SchemaStoreModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.dynamo.DynamoTestConfiguratorModule;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoSchemaTablesResource;
import io.fineo.schema.store.SchemaStore;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

/**
 * Run the kinesis record parsing using a dynamo instance to back the schema store
 */
@Category(AwsDependentTests.class)
public class TestLambdaToAvroWithDynamoStore2 extends TestLambdaToAvroWithLocalSchemaStore2 {

  @ClassRule
  public static AwsDynamoResource dynamoResource = new AwsDynamoResource();
  @Rule
  public AwsDynamoSchemaTablesResource tables = new AwsDynamoSchemaTablesResource(dynamoResource);

  /**
   * Successfully connect and create the schema store table
   *
   * @throws Exception
   */
  @Test
  public void testCreateSchemaStore() throws Exception {
    getStoreProvider().get();
    TableUtils.waitUntilExists(dynamoResource.getClient(), tables.getTestTableName(), 1000, 100);
  }

  @Override
  protected Provider<SchemaStore> getStoreProvider() throws Exception {
    Properties props = getClientProperties();
    dynamoResource.setConnectionProperties(props);
    return Guice.createInjector(
      new PropertiesModule(props),
      dynamoResource.getCredentialsModule(),
      new DynamoModule(),
      new DynamoTestConfiguratorModule(),
      new SchemaStoreModule()).getProvider(SchemaStore.class);
  }
}
