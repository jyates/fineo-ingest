package io.fineo.lambda.configure;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.lambda.kinesis.KinesisProducer;

import java.io.Serializable;

/**
 * Loads kinesis wrappers
 */
public class KinesisModule extends AbstractModule implements Serializable {
  @Override
  protected void configure() {
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

  @Provides
  public IKinesisProducer getKinesisProducer(AmazonKinesisAsyncClient client,
    @Named(LambdaClientProperties.KINESIS_RETRIES) int retries) {
    return new KinesisProducer(client, retries);
  }
}
