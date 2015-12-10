package io.fineo.lambda.avro;

import com.amazonaws.services.cloudsearchv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.schema.store.SchemaStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.ServerSocket;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Run the kinesis record parsing using a dynamo instance to back the schema store
 */
@Category(AwsDependentTests.class)
public class TestKinesisRecordToAvroWithDynamoStore
  extends TestKinesisToAvroRecordLocalSchemaStore {

  private static LocalDynamoTestUtil dynamo;
  private String testTableName;
  private static AmazonDynamoDBClient dynamodb;

  @ClassRule
  public static AwsCredentialResource credentials = new AwsCredentialResource();

  @BeforeClass
  public static void setupDb() throws Exception {
    dynamo = new LocalDynamoTestUtil(credentials);
    dynamodb = dynamo.start();
  }

  @Before
  public void selectTable() {
    testTableName = "kinesis-avro-test-" + UUID.randomUUID().toString();
  }

  @After
  public void tearDownRepository() throws Exception {
    if(storeTableCreated) {
      dynamodb.deleteTable(testTableName);
    }else{
      assertEquals("Created tables when didn't use store", 0,
        dynamodb.listTables().getTableNames().size());
    }
  }


  /**
   * Successfully connect and create the schema store table
   *
   * @throws Exception
   */
  @Test
  public void testCreateSchemaStore() throws Exception {
    getClientProperties().createSchemaStore();
    TableUtils.waitUntilExists(dynamodb, testTableName, 1000, 100);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    dynamo.stop();
  }

  @Override
  protected FirehoseClientProperties getClientProperties() throws Exception {
    Properties props = getMockProps();
    props.setProperty(FirehoseClientProperties.DYNAMO_ENDPOINT, url);
    props.setProperty(FirehoseClientProperties.DYNAMO_SCHEMA_STORE_TABLE, testTableName);
    FirehoseClientProperties fProps = new FirehoseClientProperties(props);
    fProps.setAwsCredentialProviderForTesting(credentials.getFakeProvider());
    return fProps;
  }
}
