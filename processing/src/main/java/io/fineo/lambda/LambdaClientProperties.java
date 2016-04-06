package io.fineo.lambda;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Simple wrapper around java properties
 */
public class LambdaClientProperties {

  private static final Log LOG = LogFactory.getLog(LambdaClientProperties.class);
  private static final String PROP_FILE_NAME = "fineo-lambda.properties";
  public static final String TEST_PREFIX = "fineo.integration.test.prefix";

  private final java.lang.String KINESIS_URL = "fineo.kinesis.url";
  public static final String KINESIS_PARSED_RAW_OUT_STREAM_NAME = "fineo.kinesis.parsed";
  public final String KINESIS_RETRIES = "fineo.kinesis.retries";

  public static final String DYNAMO_REGION = "fineo.dynamo.region";
  public static final String DYNAMO_URL_FOR_TESTING = "fineo.dynamo.testing.url";
  public static final String DYNAMO_SCHEMA_STORE_TABLE = "fineo.dynamo.schema-store";
  public static final String DYNAMO_INGEST_TABLE_PREFIX = "fineo.dynamo.ingest.prefix";
  public static final String DYNAMO_READ_LIMIT = "fineo.dynamo.limit.read";
  public static final String DYNAMO_WRITE_LIMIT = "fineo.dynamo.limit.write";
  public static final String DYNAMO_RETRIES = "fineo.dynamo.limit.retries";

  private AWSCredentialsProvider provider;

  private final Properties props;

  static final String FIREHOSE_URL = "fineo.firehose.url";
  /* These prefixes combine with the StreamType below to generate the full property names */
  public static final String RAW_PREFIX = "fineo.firehose.raw";
  public static final String STAGED_PREFIX = "fineo.firehose.staged";

  public enum StreamType {
    ARCHIVE("archive"), PROCESSING_ERROR("error"), COMMIT_ERROR("error.commit");

    private final String suffix;

    StreamType(String suffix) {
      this.suffix = suffix;
    }

    String getPropertyKey(String prefix) {
      return prefix + "." + suffix;
    }
  }

  /**
   * Use the static {@link #load()} to createTable properties. This is only exposed <b>FOR
   * TESTING</b>
   *
   * @param props
   */
  @VisibleForTesting
  public LambdaClientProperties(Properties props) {
    this.props = props;
  }

  public static LambdaClientProperties load() throws IOException {
    return load(PROP_FILE_NAME);
  }

  private static LambdaClientProperties load(String file) throws IOException {
    InputStream input = LambdaClientProperties.class.getClassLoader().getResourceAsStream(file);
    Preconditions.checkArgument(input != null, "Could not load properties file: " + file);
    Properties props = new Properties();
    props.load(input);
    LambdaClientProperties fProps = new LambdaClientProperties(props);
    fProps.provider = new DefaultAWSCredentialsProviderChain();
    return fProps;
  }

  public AmazonDynamoDBAsyncClient getDynamo() {
    LOG.debug("Creating dynamo with provider: " + provider);
    AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(provider);
    LOG.debug("Got client, setting endpoint");
    String region = props.getProperty(DYNAMO_REGION);
    if (region != null) {
      client.setRegion(RegionUtils.getRegion(region));
    } else {
      client.setEndpoint(props.getProperty(DYNAMO_URL_FOR_TESTING));
    }

    return client;
  }

  public SchemaStore createSchemaStore() {
    AmazonDynamoDBClient client = getDynamo();
    LOG.debug("Got dynamo client");
    CreateTableRequest create =
      DynamoDBRepository.getBaseTableCreate(getSchemaStoreTable());
    create.setProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(getDynamoReadMax())
      .withWriteCapacityUnits(getDynamoWriteMax()));
    LOG.debug("Creating schema repository");
    DynamoDBRepository repo =
      new DynamoDBRepository(new ValidatorFactory.Builder().build(), client, create);
    LOG.debug("created schema repository");
    return new SchemaStore(repo);
  }

  @VisibleForTesting
  public String getSchemaStoreTable(){
    return props.getProperty(DYNAMO_SCHEMA_STORE_TABLE);
  }

  public AmazonKinesisAsyncClient getKinesisClient() {
    AmazonKinesisAsyncClient client = new AmazonKinesisAsyncClient(provider);
    client.setEndpoint(this.getKinesisEndpoint());
    return client;
  }

  public String getKinesisEndpoint() {
    return props.getProperty(KINESIS_URL);
  }

  public String getRawToStagedKinesisStreamName() {
    return props.getProperty(KINESIS_PARSED_RAW_OUT_STREAM_NAME);
  }

  public AmazonKinesisFirehoseAsyncClient createFireHose() {
    AmazonKinesisFirehoseAsyncClient firehoseClient =
      new AmazonKinesisFirehoseAsyncClient(this.provider);
    firehoseClient.setEndpoint(this.getFirehoseUrl());
    return firehoseClient;
  }

  private String getFirehoseUrl() {
    return props.getProperty(FIREHOSE_URL);
  }

  public String getFirehoseStreamName(String phaseName, StreamType type) {
    return props.getProperty(getFirehoseStreamPropertyVisibleForTesting(phaseName, type));
  }

  @VisibleForTesting
  public static String getFirehoseStreamPropertyVisibleForTesting(String phase, StreamType type){
    return type.getPropertyKey(phase);
  }

  public String getDynamoIngestTablePrefix() {
    return props.getProperty(DYNAMO_INGEST_TABLE_PREFIX);
  }

  public Long getDynamoWriteMax() {
    return Long.valueOf(props.getProperty(DYNAMO_WRITE_LIMIT));
  }

  public Long getDynamoReadMax() {
    return Long.valueOf(props.getProperty(DYNAMO_READ_LIMIT));
  }

  public long getDynamoMaxRetries() {
    return Long.valueOf(props.getProperty(DYNAMO_RETRIES));
  }

  public long getKinesisRetries() {
    return Long.valueOf(props.getProperty(KINESIS_RETRIES));
  }

  public AWSCredentialsProvider getProvider() {
    return provider;
  }

  @VisibleForTesting
  public void setAwsCredentialProviderForTesting(AWSCredentialsProvider provider) throws Exception {
    this.provider = provider;
  }

  public static LambdaClientProperties createForTesting(Properties props,
    SchemaStore schemaStore) {
    LambdaClientProperties client = new LambdaClientProperties(props) {
      @Override
      public SchemaStore createSchemaStore() {
        return schemaStore;
      }
    };

    return client;
  }

  @VisibleForTesting
  public String getTestPrefix(){
    return props.getProperty(TEST_PREFIX);
  }
}
