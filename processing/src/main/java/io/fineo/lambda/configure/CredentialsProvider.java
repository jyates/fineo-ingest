package io.fineo.lambda.configure;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Provider;

public interface CredentialsProvider extends Provider<AWSCredentialsProvider> {

  default AWSCredentialsProvider get(){
    return getProvider();
  }

  AWSCredentialsProvider getProvider();
}
