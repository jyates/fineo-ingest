package io.fineo.lambda.avro;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
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
public class FirehoseClientProperties {

  private static final String PROP_FILE_NAME = "fineo-lambda.properties";

  private final java.lang.String KINESIS_URL = "fineo.kinesis.url";
  static final String PARSED_STREAM_NAME = "fineo.kinesis.parsed";

  static final String FIREHOSE_URL = "fineo.firehose.url";
  static final String FIREHOSE_MALFORMED_STREAM_NAME = "fineo.firehose.malformed";
  static final String FIREHOSE_STAGED_STREAM_NAME = "fineo.firehose.staged";

  static final String DYNAMO_ENDPOINT = "fineo.dynamo.url";
  static final String DYNAMO_SCHEMA_STORE_TABLE = "fineo.dynamo.schema-store";
  private static final String DYNAMO_INGEST_TABLE_PREFIX = "fineo.dynamo.ingest.prefix";
  private AWSCredentialsProvider provider;

  private final Properties props;

  @VisibleForTesting
  FirehoseClientProperties(Properties props) {
    this.props = props;
  }

  public static FirehoseClientProperties load() throws IOException {
    return load(PROP_FILE_NAME);
  }

  public static FirehoseClientProperties load(String file) throws IOException {
    InputStream input = FirehoseClientProperties.class.getClassLoader().getResourceAsStream(file);
    Preconditions.checkArgument(input != null, "Could not load properties file: " + input);
    Properties props = new Properties();
    props.load(input);
    FirehoseClientProperties fProps = new FirehoseClientProperties(props);
    fProps.provider = new DefaultAWSCredentialsProviderChain();
    return fProps;
  }

  public AmazonDynamoDBClient getDynamo(){
    AmazonDynamoDBClient client = new AmazonDynamoDBClient(provider);
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

  public static FirehoseClientProperties createForTesting(Properties props,
    SchemaStore schemaStore) {
    FirehoseClientProperties client = new FirehoseClientProperties(props) {
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
}
