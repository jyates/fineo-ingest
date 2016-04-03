package io.fineo.lambda;

import com.amazonaws.services.dynamodbv2.util.TableUtils;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.AwsDynamoResource;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

/**
 * Run the kinesis record parsing using a dynamo instance to back the schema store
 */
@Category(AwsDependentTests.class)
public class TestLambdaToAvroWithDynamoStore
  extends TestLambdaToAvroWithLocalSchemaStore {

  private static LocalDynamoTestUtil dynamo;
  private String testTableName;

  @ClassRule
  public static AwsDynamoResource dynamoResource = new AwsDynamoResource();

  @BeforeClass
  public static void setupDb() throws Exception {
    dynamo = dynamoResource.getUtil();
  }

  @Before
  public void selectTable() {
    testTableName = dynamo.getCurrentTestTable();
  }

  @After
  public void tearDownRepository() throws Exception {
    dynamo.cleanupTables(storeTableCreated);
  }


  /**
   * Successfully connect and create the schema store table
   *
   * @throws Exception
   */
  @Test
  public void testCreateSchemaStore() throws Exception {
    getClientProperties().createSchemaStore();
    TableUtils.waitUntilExists(dynamoResource.getClient(), testTableName, 1000, 100);
  }

  @Override
  protected LambdaClientProperties getClientProperties() throws Exception {
    Properties props = getMockProps();
    dynamo.setConnectionProperties(props);
    LambdaClientProperties fProps = new LambdaClientProperties(props);
    dynamoResource.setCredentials(fProps);
    return fProps;
  }
}
