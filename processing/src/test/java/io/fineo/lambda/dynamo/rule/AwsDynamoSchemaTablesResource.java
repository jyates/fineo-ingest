package io.fineo.lambda.dynamo.rule;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.configure.LambdaModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.rules.ExternalResource;

import java.util.Properties;

/**
 * Manage aws tables and getting a connection to them. Generally, this should be used at the
 * {@link org.junit.Rule} level.
 */
public class AwsDynamoSchemaTablesResource extends ExternalResource {

  private static final Log LOG = LogFactory.getLog(AwsDynamoSchemaTablesResource.class);

  private final AwsDynamoResource dynamoResource;
  private LocalDynamoTestUtil util;
  private final boolean storeTableCreatedDefault;
  private boolean storeTableCreated;
  private AmazonDynamoDBAsyncClient client;

  public AwsDynamoSchemaTablesResource(AwsDynamoResource dynamo) {
    this(dynamo, true);
  }

  public AwsDynamoSchemaTablesResource(AwsDynamoResource dynamo,
    boolean storeTablesCreatedDefault) {
    this.dynamoResource = dynamo;
    this.storeTableCreatedDefault = storeTablesCreatedDefault;
    this.storeTableCreated = storeTableCreatedDefault;
  }

  @Override
  protected void after() {
    try {
      getUtil().cleanupTables(storeTableCreated);
    } catch (ResourceNotFoundException e) {
      LOG.error("\n----------\n Could not delete a table! " + (
        storeTableCreated ? "Marked" :
        "Not marked") + " that the store table was expected to be created. Change that "
                + "expectation with #setStoreTableCreated()\n---------");
      throw e;
    }
    storeTableCreated = storeTableCreatedDefault;

    // reset any open clients
    if (client != null) {
      client.shutdown();
      client = null;
    }
  }

  public void setStoreTableCreated(boolean created) {
    this.storeTableCreated = created;
  }

  public String getTestTableName() {
    return getUtil().getCurrentTestTable();
  }

  public LambdaClientProperties getClientProperties() throws Exception {
    return getClientProperties(new Properties());
  }

  public LambdaClientProperties getClientProperties(Properties props) throws Exception {
    getUtil().setConnectionProperties(props);
    LambdaClientProperties fProps = LambdaClientProperties
      .create(new LambdaModule(props), dynamoResource.getCredentialsModule(),
        getDynamoModule());
    return fProps;
  }

  public void setConnectionProperties(Properties props){
    getUtil().setConnectionProperties(props);
  }

  public AbstractModule getDynamoModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
      }

      @Provides
      public AmazonDynamoDBAsyncClient getDynamo() {
        return getAsyncClient();
      }
    };
  }

  public AmazonDynamoDBAsyncClient getAsyncClient() {
    if (this.client == null) {
      this.client = getUtil().getAsyncClient();
    }
    return this.client;
  }

  private LocalDynamoTestUtil getUtil() {
    if (this.util == null) {
      this.util = dynamoResource.getUtil();
    }
    return this.util;
  }
}
