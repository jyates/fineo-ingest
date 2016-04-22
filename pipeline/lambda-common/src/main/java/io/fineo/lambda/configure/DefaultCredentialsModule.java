package io.fineo.lambda.configure;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.google.inject.AbstractModule;

import java.io.Serializable;
import java.util.Properties;

/**
 * Loads the default AWS credentials
 */
public class DefaultCredentialsModule extends AbstractModule implements Serializable {

  private final AWSCredentialsProvider credentials;

  public DefaultCredentialsModule() {
    this(null);
  }

  private DefaultCredentialsModule(String credKey) {
    this.credentials = credKey != null ?
                       new ProfileCredentialsProvider(credKey) :
                       new DefaultAWSCredentialsProviderChain();
  }

  @Override
  protected void configure() {
    bind(AWSCredentialsProvider.class).toInstance(credentials);
  }

  public static DefaultCredentialsModule create(Properties props) {
    String cred = props.getProperty("fineo.aws.testing.creds");
    return new DefaultCredentialsModule(cred);
  }
}
