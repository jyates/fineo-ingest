package io.fineo.lambda.configure.dynamo;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DynamoModule extends AbstractModule {
  private static final Log LOG = LogFactory.getLog(DynamoModule.class);

  @Provides
  @Inject
  @Singleton
  public AmazonDynamoDBAsyncClient getDynamoClient(AWSCredentialsProvider provider,
    AwsDynamoConfigurator configurator) {
    LOG.debug("Creating dynamo with provider: " + provider);
    AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(provider);
    configurator.configure(client);
    LOG.debug("Got client, setting endpoint");
    return client;
  }

  @Provides
  @Inject
  @Singleton
  public DynamoDB getDynamoDB(AmazonDynamoDBAsyncClient client) {
    return new DynamoDB(client);
  }

  @Override
  protected void configure() {
  }
}
