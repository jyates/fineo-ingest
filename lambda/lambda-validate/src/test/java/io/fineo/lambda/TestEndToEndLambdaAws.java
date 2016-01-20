package io.fineo.lambda;

import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DeleteDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.S3DestinationConfiguration;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.LogType;
import com.google.common.collect.Lists;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertNull;

/**
 * Similar to the local TestEndToEndLambda, but leverages actual AWS services to support
 * access
 */
@Category(AwsDependentTests.class)
public class TestEndToEndLambdaAws {

  private static final Log LOG = LogFactory.getLog(TestEndToEndLambdaAws.class);

  @ClassRule
  public static AwsCredentialResource awsCredentials = new AwsCredentialResource();
  private final String region = System.getProperty("aws-region", "us-east-1");

  private static final String TEST_EVENT =
    "{"
    + "  \"Records\": ["
    + "    {"
    + "      \"eventID\": "
    + "\"shardId-000000000000:49545115243490985018280067714973144582180062593244200961\","
    + "      \"eventVersion\": \"1.0\","
    + "      \"kinesis\": {"
    + "        \"partitionKey\": \"partitionKey-3\","
    + "        \"data\": \"%s\","
    + "        \"kinesisSchemaVersion\": \"1.0\","
    + "        \"sequenceNumber\": \"49545115243490985018280067714973144582180062593244200961\""
    + "      },"
    + "      \"invokeIdentityArn\": \"arn:aws:iam::EXAMPLE\","
    + "      \"eventName\": \"aws:kinesis:record\","
    + "      \"eventSourceARN\": \"arn:aws:kinesis:EXAMPLE\","
    + "      \"eventSource\": \"aws:kinesis\","
    + "      \"awsRegion\": \"%s\""
    + "    }"
    + "  ]"
    + "}"
    + "";
  private LambdaClientProperties props;

  private static final String FUNCTION_ARN =
    "arn:aws:lambda:us-east-1:766732214526:function:RawToAvro";
  /**
   * test role for the Firehose stream -> S3 bucket
   */
  private String firehoseToS3RoleArn = "arn:aws:iam::766732214526:role/test-lambda-functions";
  private String s3Bucket = "arn:aws:s3:::test.fineo.io";

  @Test
  public void testConnect() throws Exception {
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    setupAws(json);

    LOG.info("------ > Making request ----->");
    makeRequest(json);

    validate();
  }

  @Before
  public void connect() throws Exception {
    this.props = LambdaClientProperties.load();
    props.setAwsCredentialProviderForTesting(awsCredentials.getProvider());
  }

  @After
  public void cleanup() throws Exception {
    // dynamo
    ListTablesResult tables = props.getDynamo().listTables("test-schema");
    for (String name : tables.getTableNames()) {
      props.getDynamo().deleteTable(name);
    }

    // firehose
    AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(awsCredentials
      .getProvider());
    Stream.of(props.getFirehoseStagedStreamName()).forEach(stream ->{
      DeleteDeliveryStreamRequest delete = new DeleteDeliveryStreamRequest()
        .withDeliveryStreamName(stream);
      firehoseClient.deleteDeliveryStream(delete);
    });
  }

  private void setupAws(Map<String, Object> json) throws Exception {
    // setup the schema store
    SchemaStore store = props.createSchemaStore();
    LambdaTestUtils.updateSchemaStore(store, json);

    // setup the firehose connections
    String prefix = LocalDateTime.now().toString();
    createFirehose(props.getFirehoseStagedStreamName(), prefix);
  }

  private void createFirehose(String stream, String prefix) {
    AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(awsCredentials
      .getProvider());
    CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest();
    createDeliveryStreamRequest.setDeliveryStreamName(stream);

    S3DestinationConfiguration s3DestinationConfiguration = new S3DestinationConfiguration();
    s3DestinationConfiguration.setBucketARN(s3Bucket);
    s3DestinationConfiguration.setPrefix(prefix);
    // Could also specify GZIP, ZIP, or SNAPPY
    s3DestinationConfiguration.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
    s3DestinationConfiguration.setRoleARN(firehoseToS3RoleArn);
    createDeliveryStreamRequest.setS3DestinationConfiguration(s3DestinationConfiguration);
    firehoseClient.createDeliveryStream(createDeliveryStreamRequest);
  }

  private void makeRequest(Map<String, Object> json) throws IOException, OldSchemaException {
    ByteBuffer bytes = LambdaTestUtils.asBytes(json);
    String data = Base64.getEncoder().encodeToString(Arrays.copyOfRange(bytes.array(), bytes
      .arrayOffset(), bytes.remaining()));
    LOG.info("Data: " + data);

    InvokeRequest request = new InvokeRequest();
    request.setFunctionName(FUNCTION_ARN);
    request.withPayload(String.format(TEST_EVENT, data, region));
    request.setInvocationType(InvocationType.RequestResponse);
    request.setLogType(LogType.Tail);

    AWSLambdaClient client = new AWSLambdaClient(awsCredentials.getProvider());
    InvokeResult result = client.invoke(request);
    LOG.info("Status: " + result.getStatusCode());
    LOG.info("Error:" + result.getFunctionError());
    LOG.info("Log:" + new String(Base64.getDecoder().decode(result.getLogResult())));
    assertNull(result.getFunctionError());
  }

  private void validate() {

  }
}