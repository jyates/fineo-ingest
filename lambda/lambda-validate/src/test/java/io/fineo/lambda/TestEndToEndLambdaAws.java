package io.fineo.lambda;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DeleteDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.S3DestinationConfiguration;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.LogType;
import com.google.common.base.Preconditions;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.Assert.assertNull;

/**
 * Similar to the local TestEndToEndLambda, but leverages actual AWS services to support
 * access
 */
@Category(AwsDependentTests.class)
public class TestEndToEndLambdaAws {

  private static final Log LOG = LogFactory.getLog(TestEndToEndLambdaAws.class);
  public static final int TEN_SECONDS = 10000;
  public static final int ONE_SECOND = 1000;

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
  private List<String> firehoses = new ArrayList<>();
  private Integer kinesisShardCount = 1;
  private List<String> kinesisStreams = new ArrayList<>();

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
    ListTablesResult tables = props.getDynamo().listTables(props.getTestPrefix());
    for (String name : tables.getTableNames()) {
      props.getDynamo().deleteTable(name);
    }

    // firehose
    AmazonKinesisFirehoseClient firehoseClient =
      new AmazonKinesisFirehoseClient(awsCredentials.getProvider());
    firehoses.stream().forEach(stream -> {
      DeleteDeliveryStreamRequest delete = new DeleteDeliveryStreamRequest()
        .withDeliveryStreamName(stream);
      firehoseClient.deleteDeliveryStream(delete);
    });

    // kinesis
    AmazonKinesisClient client = getKinesis();
    kinesisStreams.stream().forEach(name -> {
      DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
      deleteStreamRequest.setStreamName(name);
      client.deleteStream(deleteStreamRequest);
    });

  }

  private void setupAws(Map<String, Object> json) throws Exception {
    // setup the schema store
    SchemaStore store = props.createSchemaStore();
    LambdaTestUtils.updateSchemaStore(store, json);

    // setup the firehose connections
    String prefix = LocalDateTime.now().toString();
    createFirehose(props.getFirehoseStream(LambdaClientProperties.RAW_PREFIX,
      LambdaClientProperties.StreamType.ARCHIVE), prefix);

    // setup the kinesis streams
    AmazonKinesisClient client = getKinesis();
    CreateStreamRequest createStreamRequest = new CreateStreamRequest();
    String stream = props.getRawToStagedKinesisStreamName();
    kinesisStreams.add(stream);
    createStreamRequest.setStreamName(stream);
    createStreamRequest.setShardCount(kinesisShardCount);
    client.createStream(createStreamRequest);
    waitForStreamToBeCreated(client, createStreamRequest.getStreamName(), TEN_SECONDS, ONE_SECOND);
  }

  private AmazonKinesisClient getKinesis() {
    AmazonKinesisClient client = new AmazonKinesisClient(awsCredentials.getProvider());
    client.setRegion(RegionUtils.getRegion(region));
    return client;
  }

  private void createFirehose(String stream, String prefix) throws InterruptedException {
    firehoses.add(stream);
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
    waitForCreation(() -> {
      DescribeDeliveryStreamRequest describe = new DescribeDeliveryStreamRequest()
        .withDeliveryStreamName(stream);
      DescribeDeliveryStreamResult result = firehoseClient.describeDeliveryStream(describe);
      return result.getDeliveryStreamDescription().getDeliveryStreamStatus();
    }, status -> status.equals("ACTIVE"), TEN_SECONDS, ONE_SECOND);
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

  private void waitForStreamToBeCreated(AmazonKinesisClient client, String stream, int timeoutMs,
    int intervalMs) throws InterruptedException, IllegalArgumentException {
    waitForCreation(() -> {
      DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
      describeStreamRequest.setStreamName(stream);
      DescribeStreamResult describeStreamResponse =
        client.describeStream(describeStreamRequest);
      return describeStreamResponse.getStreamDescription().getStreamStatus();
    }, streamStatus -> streamStatus.equals("ACTIVE"), timeoutMs, intervalMs);
  }

  private <RESULT> void waitForCreation(Supplier<RESULT> describer, Predicate<RESULT> success,
    int timeoutMs, int intervalMs) throws InterruptedException {
    Preconditions.checkArgument(timeoutMs > 0, "Timeout must be >= 0");
    if (intervalMs > 0 && intervalMs < timeoutMs) {
      long startTime = System.currentTimeMillis();
      long endTime = startTime + (long) timeoutMs;
      for (; System.currentTimeMillis() < endTime; Thread.sleep((long) intervalMs)) {
        try {
          if (success.test(describer.get())) {
            break;
          }
        } catch (ResourceNotFoundException var11) {
        }
      }
    } else {
      throw new IllegalArgumentException("Interval must be > 0 and < timeout");
    }
  }

  private void validate() {

  }
}