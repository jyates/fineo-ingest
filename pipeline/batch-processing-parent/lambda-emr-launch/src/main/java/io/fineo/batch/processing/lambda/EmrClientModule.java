package io.fineo.batch.processing.lambda;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;

public class EmrClientModule extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  @Inject
  public AmazonElasticMapReduceClient getEmrClient(AWSCredentialsProvider credentials){
    return  new AmazonElasticMapReduceClient(credentials);
  }
}
