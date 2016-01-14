package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.avro.LambdaClientProperties;
import io.fineo.lambda.avro.LocalDynamoTestUtil;
import org.junit.rules.ExternalResource;

import java.util.Properties;

/**
 * Helper resource to standup and shutdown a local dynamo instance
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
