package io.fineo.lambda.dynamo.rule;

import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.rules.ExternalResource;

import java.util.Properties;

/**
 * Manage aws tables and getting a connection to them. Generally, this should be used at the
 * {@link org.junit.Rule} level.
 */
public class AwsDynamoTablesResource extends ExternalResource {

  private static final Log LOG = LogFactory.getLog(AwsDynamoTablesResource.class);

  private final AwsDynamoResource dynamoResource;
  private LocalDynamoTestUtil util;
  private final boolean storeTableCreatedDefault = true;
  private boolean storeTableCreated = storeTableCreatedDefault;

  public AwsDynamoTablesResource(AwsDynamoResource dynamo) {
    this.dynamoResource = dynamo;
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
    LambdaClientProperties fProps = new LambdaClientProperties(props);
    dynamoResource.setCredentials(fProps);
    return fProps;
  }

  private LocalDynamoTestUtil getUtil() {
    if (this.util == null) {
      this.util = dynamoResource.getUtil();
    }
    return this.util;
  }
}
