package io.fineo.lambda.configure.dynamo;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DynamoModule extends AvroToDynamoModule{
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
}
