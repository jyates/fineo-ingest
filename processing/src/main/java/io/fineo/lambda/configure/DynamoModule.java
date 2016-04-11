package io.fineo.lambda.configure;

import com.amazonaws.regions.RegionUtils;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.name.Named;

public class DynamoModule extends AbstractModule {
  private String region;
  private String endpoint;

  @Override
  protected void configure() {
  }

  @Provides
  public AwsDynamoConfigurator getDynamoConfig() {
    return client -> {
      if (region != null) {
        client.setRegion(RegionUtils.getRegion(region));
      } else {
        client.setEndpoint(endpoint);
      }
    };
  }

  @Inject(optional = true)
  public void setDynamoRegion(@Named(LambdaClientProperties.DYNAMO_REGION) String region) {
    this.region = region;
  }


  @Inject(optional = true)
  public void setDynamoEndpointForTesting(
    @Named(LambdaClientProperties.DYNAMO_URL_FOR_TESTING) String endpoint) {
    this.endpoint = endpoint;
  }
}
