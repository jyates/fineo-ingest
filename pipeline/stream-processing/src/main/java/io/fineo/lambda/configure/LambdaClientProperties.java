package io.fineo.lambda.configure;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import io.fineo.schema.store.SchemaStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Simple wrapper around java properties
 */
public class LambdaClientProperties {

  private static final String PROP_FILE_NAME = "fineo-lambda.properties";
  public static final String TEST_PREFIX = "fineo.integration.test.prefix";

  public final java.lang.String KINESIS_URL = "fineo.kinesis.url";
  public static final String KINESIS_PARSED_RAW_OUT_STREAM_NAME = "fineo.kinesis.parsed";
  public final String KINESIS_RETRIES = "fineo.kinesis.retries";

  public static final String DYNAMO_REGION = "fineo.dynamo.region";
  public static final String DYNAMO_URL_FOR_TESTING = "fineo.dynamo.testing.url";
  public static final String DYNAMO_SCHEMA_STORE_TABLE = "fineo.dynamo.schema-store";
  public static final String DYNAMO_INGEST_TABLE_PREFIX = "fineo.dynamo.ingest.prefix";
  public static final String DYNAMO_READ_LIMIT = "fineo.dynamo.limit.read";
  public static final String DYNAMO_WRITE_LIMIT = "fineo.dynamo.limit.write";
  public static final String DYNAMO_RETRIES = "fineo.dynamo.limit.retries";

  public static final String FIREHOSE_URL = "fineo.firehose.url";
  /* These prefixes combine with the StreamType below to generate the full property names */
  public static final String RAW_PREFIX = "fineo.firehose.raw";
  public static final String STAGED_PREFIX = "fineo.firehose.staged";

  private Provider<AWSCredentialsProvider> credentials;
  private Provider<AmazonDynamoDBAsyncClient> dynamo;
  private Provider<SchemaStore> storeProvider;
  private Properties props;

  /**
   * Use the static {@link #load()}. This is only exposed <b>FOR TESTING</b> and injection
   */
  @VisibleForTesting
  public LambdaClientProperties() {
  }

  @Inject(optional = true)
  public void setCredentials(Provider<AWSCredentialsProvider> credentials) {
    this.credentials = credentials;
  }

  @Inject(optional = true)
  public void setDynamo(Provider<AmazonDynamoDBAsyncClient> dynamo) {
    this.dynamo = dynamo;
  }

  @Inject(optional = true)
  public void setStoreProvider(Provider<SchemaStore> storeProvider) {
    this.storeProvider = storeProvider;
  }

  @Inject(optional = true)
  public void setProps(Properties props) {
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
    Injector injector =
      Guice.createInjector(new LambdaModule(props), new DefaultCredentialsModule(),
        new DynamoModule(), new DynamoRegionConfigurator());
    return injector.getInstance(LambdaClientProperties.class);
  }

  public static LambdaClientProperties create(AbstractModule... modules) {
    Injector injector = Guice.createInjector(modules);
    return injector.getInstance(LambdaClientProperties.class);
  }

  public AmazonDynamoDBAsyncClient getDynamo() {
    return dynamo.get();
  }

  public SchemaStore createSchemaStore() {
    return storeProvider.get();
  }

  @VisibleForTesting
  public String getSchemaStoreTable() {
    return props.getProperty(DYNAMO_SCHEMA_STORE_TABLE);
  }

  public AmazonKinesisAsyncClient getKinesisClient() {
    AmazonKinesisAsyncClient client = new AmazonKinesisAsyncClient(credentials.get());
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
      new AmazonKinesisFirehoseAsyncClient(this.credentials.get());
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
  public static String getFirehoseStreamPropertyVisibleForTesting(String phase, StreamType type) {
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
    return credentials.get();
  }

  public static LambdaClientProperties createForTesting(Properties props,
    SchemaStore schemaStore) {
    LambdaClientProperties lProps = new LambdaClientProperties();
    lProps.setProps(props);
    lProps.setStoreProvider(() -> schemaStore);
    return lProps;
  }

  @VisibleForTesting
  public String getTestPrefix() {
    return props.getProperty(TEST_PREFIX);
  }

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
}
