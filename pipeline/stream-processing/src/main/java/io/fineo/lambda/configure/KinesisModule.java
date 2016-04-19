package io.fineo.lambda.configure;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.kinesis.KinesisProducer;

/**
 * Loads kinesis wrappers
 */
public class KinesisModule extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  public KinesisProducer getKinesisProducer(AmazonKinesisAsyncClient client,
    @Named(LambdaClientProperties.KINESIS_RETRIES) int retries) {
    return new KinesisProducer(client, retries);
  }
}
