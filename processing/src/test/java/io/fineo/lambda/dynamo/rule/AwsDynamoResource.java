package io.fineo.lambda.dynamo.rule;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import org.junit.rules.ExternalResource;

import java.util.Properties;

/**
 * Helper resource to standup and shutdown a local dynamo instance.
 * <p>
 * This handles setting up a local DynamoDB when the rule is created and shutting down the
 * database when the rule is stopped. If you are using {@link org.junit.ClassRule}, then the
 * database will be up before the {@link org.junit.BeforeClass} method is called and shutdown
 * after {@link org.junit.AfterClass}.
 * </p>
 * <p>
 * Sometimes, you will want to remove tables between tests. In that case, you can call {@link
 * #cleanup()} to remove all the current tables
 * </p>
 */
public class AwsDynamoResource extends ExternalResource {

  private AwsCredentialResource credentials = new AwsCredentialResource();
  private LocalDynamoTestUtil util;

  private LocalDynamoTestUtil start() throws Exception {
    util = new LocalDynamoTestUtil(credentials);
    util.start();
    return util;
  }

  @Override
  protected void before() throws Exception {
    start();
  }

  @Override
  protected void after() {
    if (util != null) {
      try {
        util.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public LocalDynamoTestUtil getUtil() {
    return this.util;
  }

  public AwsCredentialResource getCredentials() {
    return credentials;
  }

  public AmazonDynamoDBClient getClient() {
    return util.getClient();
  }

  public AmazonDynamoDBAsyncClient getAsyncClient() {
    return util.getAsyncClient();
  }

  public void setConnectionProperties(Properties prop) {
    util.setConnectionProperties(prop);
  }

  public void setCredentials(LambdaClientProperties props) throws Exception {
    props.setAwsCredentialProviderForTesting(this.getCredentials().getFakeProvider());
  }

  public void cleanup() {
    this.util.cleanupTables(false);
  }
}
