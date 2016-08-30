package io.fineo.batch.processing.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.configure.NullableNamedInstanceModule;
import io.fineo.lambda.dynamo.TableUtils;

import java.util.Properties;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig
  .TableNameOverride.withTableNameReplacement;

public class IngestManifestModule extends NullableNamedInstanceModule {

  public static final String INGEST_MANIFEST_OVERRIDE = "fineo.dynamo.batch-manifest.table";
  public static final String READ_LIMIT = "fineo.dynamo.batch-manifest.limit.read";
  public static final String WRITE_LIMIT = "fineo.dynamo.batch-manifest.limit.write";
  private boolean createTable;

  public static IngestManifestModule create(Properties props) {
    String override = props.getProperty(IngestManifestModule.INGEST_MANIFEST_OVERRIDE);
    return override == null ?
           new IngestManifestModule() :
           new IngestManifestModule(withTableNameReplacement(override));
  }

  public static IngestManifestModule createForTesting() {
    return new IngestManifestModule().createTableForTesting();
  }

  private IngestManifestModule createTableForTesting() {
    this.createTable = true;
    return this;
  }

  public static IngestManifestModule createForTesting(String override) {
    return new IngestManifestModule(withTableNameReplacement(override));
  }

  private IngestManifestModule() {
    this(null);
  }

  private IngestManifestModule(DynamoDBMapperConfig.TableNameOverride override) {
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
      .withTableNameOverride(override == null ? null : override.get()).build());
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
    CreateTableRequest create = getCreate(mapper);
    if (createTable) {
      ensureTable(create, dynamo, throughput);
    }
    return new IngestManifest(mapper, create.getTableName(), submitter);
  }

  private void ensureTable(CreateTableRequest create, AmazonDynamoDBAsyncClient dynamo,
    Provider<ProvisionedThroughput> throughput) {
    try {
      dynamo.describeTable(create.getTableName());
    } catch (ResourceNotFoundException e) {
      create.setProvisionedThroughput(throughput.get());
      TableUtils.createTable(new DynamoDB(dynamo), create);
    }
  }

  private CreateTableRequest getCreate(DynamoDBMapper mapper) {
    return mapper.generateCreateTableRequest(DynamoIngestManifest.class);
  }
}
