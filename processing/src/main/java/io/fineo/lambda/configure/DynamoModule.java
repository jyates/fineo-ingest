package io.fineo.lambda.configure;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;

public class DynamoModule extends AbstractModule {

  @Override
  protected void configure() {
  }

  @Provides
  public AwsDynamoConfigurator getDynamoConfig(
    @Named(LambdaClientProperties.DYNAMO_REGION) String region) {
    return new AwsDynamoConfigurator() {
      @Override
      public void configure(AmazonDynamoDBAsyncClient client) {
        client.setRegion(RegionUtils.getRegion(region));
      }
    };
  }
}
