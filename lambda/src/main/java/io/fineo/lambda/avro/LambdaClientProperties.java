package io.fineo.lambda.avro;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Simple wrapper around java properties
 */
public class LambdaClientProperties {

  private static final String PROP_FILE_NAME = "fineo-lambda.properties";

  private final java.lang.String KINESIS_URL = "fineo.kinesis.url";
  public static final String PARSED_STREAM_NAME = "fineo.kinesis.parsed";

  static final String FIREHOSE_URL = "fineo.firehose.url";
  public static final String FIREHOSE_MALFORMED_STREAM_NAME = "fineo.firehose.malformed";
  public static final String FIREHOSE_STAGED_STREAM_NAME = "fineo.firehose.staged";
  public static final String FIREHOSE_STAGED_DYANMO_ERROR_STREAM_NAME = "firehose.staged.error.dynamo";

  public static final String DYNAMO_ENDPOINT = "fineo.dynamo.url";
  public static final String DYNAMO_SCHEMA_STORE_TABLE = "fineo.dynamo.schema-store";
  public static final String DYNAMO_INGEST_TABLE_PREFIX = "fineo.dynamo.ingest.prefix";
  public static final String DYNAMO_READ_LIMIT = "fineo.dynamo.limit.read";
  public static final String DYNAMO_WRITE_LIMIT = "fineo.dynamo.limit.write";
  public static final String DYNAMO_RETRIES = "fineo.dynamo.limit.retries";

  private AWSCredentialsProvider provider;

  private final Properties props;

  /**
   * Use the static {@link #load()} to createTable properties. This is only exposed <b>FOR TESTING</b>
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
    Preconditions.checkArgument(input != null, "Could not load properties file: " + input);
    Properties props = new Properties();
    props.load(input);
    LambdaClientProperties fProps = new LambdaClientProperties(props);
    fProps.provider = new DefaultAWSCredentialsProviderChain();
    return fProps;
  }

  public AmazonDynamoDBAsyncClient getDynamo() {
    AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(provider);
    client.setEndpoint(props.getProperty(DYNAMO_ENDPOINT));
    return client;
  }

  public SchemaStore createSchemaStore() {
    AmazonDynamoDBClient client = getDynamo();
    DynamoDBRepository repo =
      new DynamoDBRepository(client, props.getProperty(DYNAMO_SCHEMA_STORE_TABLE),
        new ValidatorFactory.Builder().build());
    return new SchemaStore(repo);
  }

  public String getKinesisEndpoint() {
    return props.getProperty(KINESIS_URL);
  }

  public String getParsedStreamName() {
    return props.getProperty(PARSED_STREAM_NAME);
  }


  public String getFirehoseUrl() {
    return props.getProperty(FIREHOSE_URL);
  }

  public String getFirehoseMalformedStreamName() {
    return props.getProperty(FIREHOSE_MALFORMED_STREAM_NAME);
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

  public String getFirehoseStagedStreamName() {
    return props.getProperty(FIREHOSE_STAGED_STREAM_NAME);
  }

  public String getDynamoIngestTablePrefix() {
    return props.getProperty(DYNAMO_INGEST_TABLE_PREFIX);
  }

  public String getFirehoseStagedDyanmoErrorsName() {
    return props.getProperty(FIREHOSE_STAGED_DYANMO_ERROR_STREAM_NAME);
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
}
