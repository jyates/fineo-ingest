package io.fineo.batch.processing.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.name.Named;

/**
 *
 */
public class IngestManifestModule extends AbstractModule {

  @Override
  protected void configure() {
  }

  @Provides
  @Inject
  public DynamoDBMapper getMapper(AmazonDynamoDBAsyncClient client,
    @Named("ingest.manifest.override") DynamoDBMapperConfig.TableNameOverride override) {
    return new DynamoDBMapper(client, new DynamoDBMapperConfig.Builder()
      .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
      .withPaginationLoadingStrategy(DynamoDBMapperConfig.PaginationLoadingStrategy.EAGER_LOADING)
      .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.APPEND_SET)
      .withTableNameOverride(override).build());
  }
}
