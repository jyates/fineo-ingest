package io.fineo.lambda.configure;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.dynamo.AwsDynamoConfigurator;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Base Module for loading connections to AWS components
 */
public class AwsBaseComponentModule extends AbstractModule {
  private static final Log LOG = LogFactory.getLog(AwsBaseComponentModule.class);

  @Override
  protected void configure() {
  }

  @Provides
  @Inject
  @Singleton
  public AmazonKinesisFirehoseAsyncClient getFirehoseClient(AWSCredentialsProvider credentials,
    @Named("fineo.firehose.url") String url) {
    AmazonKinesisFirehoseAsyncClient client = new AmazonKinesisFirehoseAsyncClient(credentials);
    client.setEndpoint(url);
    return client;
  }

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
  public AmazonKinesisAsyncClient getKinesisClient(AWSCredentialsProvider provider,
    @Named(LambdaClientProperties.KINESIS_URL) String url) {
    AmazonKinesisAsyncClient client = new AmazonKinesisAsyncClient(provider);
    client.setEndpoint(url);
    return client;
  }
}
