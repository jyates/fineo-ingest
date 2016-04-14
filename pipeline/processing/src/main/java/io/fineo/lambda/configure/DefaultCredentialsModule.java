package io.fineo.lambda.configure;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.inject.AbstractModule;

/**
 * Loads the default AWS credentials
 */
public class DefaultCredentialsModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(AWSCredentialsProvider.class).toInstance(new DefaultAWSCredentialsProviderChain());
  }
}
