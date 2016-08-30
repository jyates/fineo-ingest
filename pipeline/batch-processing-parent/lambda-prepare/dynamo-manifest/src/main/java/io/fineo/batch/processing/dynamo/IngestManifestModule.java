package io.fineo.batch.processing.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.configure.NullableNamedInstanceModule;

public class IngestManifestModule extends NullableNamedInstanceModule {

  public static final String INGEST_MANIFEST_OVERRIDE = "fineo.dynamo.batch-manifest.table";
  public static final String READ_LIMIT = "fineo.dynamo.batch-manifest.limit.read";
  public static final String WRITE_LIMIT = "fineo.dynamo.batch-manifest.limit.write";

  public IngestManifestModule() {
    this(null);
  }

  public IngestManifestModule(DynamoDBMapperConfig.TableNameOverride override) {
    super(INGEST_MANIFEST_OVERRIDE, override, DynamoDBMapperConfig.TableNameOverride.class);
  }

  @Provides
  @Inject
  @Singleton
  public ProvisionedThroughput getThroughput(@Named(READ_LIMIT) long readLimit,
    @Named(WRITE_LIMIT) long writeLimit) {
    return new ProvisionedThroughput(readLimit, writeLimit);
  }

  @Provides
  @Inject
  @Singleton
  public DynamoDBMapper getMapper(AmazonDynamoDBAsyncClient client,
    @Named(INGEST_MANIFEST_OVERRIDE) Provider<DynamoDBMapperConfig.TableNameOverride> override) {
    return new DynamoDBMapper(client, new DynamoDBMapperConfig.Builder()
      .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
      .withPaginationLoadingStrategy(DynamoDBMapperConfig.PaginationLoadingStrategy.EAGER_LOADING)
      .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.APPEND_SET)
      .withTableNameOverride(override == null? null: override.get()).build());
  }

  @Provides
  @Inject
  @Singleton
  public AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, DynamoIngestManifest> getSubmitter(
    AmazonDynamoDBAsyncClient client) {
    return new AwsAsyncSubmitter<>(1, client::updateItemAsync);
  }

  @Provides
  @Inject
  @Singleton
  public IngestManifest getManifest(DynamoDBMapper mapper, AmazonDynamoDBAsyncClient dynamo,
    Provider<ProvisionedThroughput> throughput,
    AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, DynamoIngestManifest> submitter) {
    return new IngestManifest(mapper, dynamo, throughput, submitter);
  }
}
