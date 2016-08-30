package io.fineo.batch.processing.spark.options;

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.name.Named;

/**
 * Create provisioned throughput for a Named read and write limit
 */
public class DynamoProvisionedThroughputModule extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  @Inject
  public ProvisionedThroughput getThroughput(@Named("fineo.dynamo.batch-manifest.throughput.read") long read,
    @Named("fineo.dynamo.batch-manifest.throughput.read") long write) {
    return new ProvisionedThroughput(read, write);
  }
}
