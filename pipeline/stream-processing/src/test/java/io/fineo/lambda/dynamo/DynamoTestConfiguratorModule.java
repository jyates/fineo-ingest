package io.fineo.lambda.dynamo;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.dynamo.AwsDynamoConfigurator;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;

public class DynamoTestConfiguratorModule extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  @Inject
  public AwsDynamoConfigurator getTestConfigurator(
    @Named(LambdaClientProperties.DYNAMO_URL_FOR_TESTING) String url) {
    return client -> client.setEndpoint(url);
  }
}
