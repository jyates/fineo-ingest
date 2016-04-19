package io.fineo.lambda.configure.dynamo;

import com.amazonaws.regions.RegionUtils;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;

/**
 * A module that loads a {@link AwsDynamoConfigurator} to configure with a given region
 */
public class DynamoRegionConfigurator extends AbstractModule {

  @Override
  protected void configure() {
  }

  @Provides
  @Inject
  public AwsDynamoConfigurator getRegionConfigurator(
    @Named(LambdaClientProperties.DYNAMO_REGION) String region) {
    return client -> client.setRegion(RegionUtils.getRegion(region));
  }
}
