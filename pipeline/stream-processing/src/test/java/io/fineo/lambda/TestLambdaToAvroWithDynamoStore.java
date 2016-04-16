package io.fineo.lambda;

import com.amazonaws.services.dynamodbv2.util.TableUtils;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.configure.LambdaModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.dynamo.rule.AwsDynamoSchemaTablesResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

/**
 * Run the kinesis record parsing using a dynamo instance to back the schema store
 */
@Category(AwsDependentTests.class)
public class TestLambdaToAvroWithDynamoStore extends TestLambdaToAvroWithLocalSchemaStore {

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
    getClientProperties().createSchemaStore();
    TableUtils.waitUntilExists(dynamoResource.getClient(), tables.getTestTableName(), 1000, 100);
  }

  @Override
  protected LambdaClientProperties getClientProperties() throws Exception {
    Properties props = getMockProps();
    return tables.getClientProperties(props);
  }
}
