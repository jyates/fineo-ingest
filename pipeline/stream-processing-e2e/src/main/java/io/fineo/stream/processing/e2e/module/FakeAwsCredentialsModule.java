package io.fineo.stream.processing.e2e.module;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

/**
 * Module for providing fake aws credentials. Used to access dynamo
 */
public class FakeAwsCredentialsModule extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  @Singleton
  public AWSCredentialsProvider getCredentials() {
    return new StaticCredentialsProvider(
      new BasicAWSCredentials("AKIAIZFKPYAKBFDZPAEA", "18S1bF4bpjCKZP2KRgbqOn7xJLDmqmwSXqq5GAWq"));
  }

}
