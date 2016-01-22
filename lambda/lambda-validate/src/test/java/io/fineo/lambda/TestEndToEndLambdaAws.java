package io.fineo.lambda;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.util.EndToEndTestUtil;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Similar to the local TestEndToEndLambda, but leverages actual AWS services to support
 * access
 */
@Category(AwsDependentTests.class)
public class TestEndToEndLambdaAws {

  private static final Log LOG = LogFactory.getLog(TestEndToEndLambdaAws.class);
  public static final int ONE_SECOND = 1000;
  public static final int THREE_HUNDRED_SECONDS = 3 * 100 * ONE_SECOND;

  @ClassRule
  public static AwsCredentialResource awsCredentials = new AwsCredentialResource();
  private final String region = System.getProperty("aws-region", "us-east-1");
  private static final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
    MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));

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
  private String s3BucketName = "test.fineo.io";
  private String s3BucketArn = "arn:aws:s3:::" + s3BucketName;
  private List<String> firehoses = Collections.synchronizedList(new ArrayList<>());

  private Integer kinesisShardCount = 1;
  private List<String> kinesisStreams = Collections.synchronizedList(new ArrayList<>());

  @Before
  public void connect() throws Exception {
    this.props = LambdaClientProperties.load();
    props.setAwsCredentialProviderForTesting(awsCredentials.getProvider());
  }

  @Test
  public void testConnect() throws Exception {
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    TestProgress progress = new TestProgress();
    progress.event = json;
    setupAws(progress);

    makeRequest(progress);

    validate(progress);
  }

  @After
  public void cleanup() throws Exception {
    // dynamo
    ListTablesResult tables = props.getDynamo().listTables(props.getTestPrefix());
    for (String name : tables.getTableNames()) {
      if (!name.startsWith(props.getTestPrefix())) {
        LOG.debug("Stopping deletion of dynamo tables with name: " + name);
        break;
      }
      LOG.debug("Deleting dynamo table: " + name);
      props.getDynamo().deleteTable(name);
    }

    // remove all s3 files with the current test prefix
    AmazonS3 s3 = new AmazonS3Client(awsCredentials.getProvider());
    ObjectListing listing = s3.listObjects(s3BucketName, props.getTestPrefix());
    listing.getObjectSummaries().stream()
           .peek(s -> LOG.info("Deleting " + s3BucketName + "/" + s.getKey()))
           .map(summary -> {
             s3.deleteObject(s3BucketName, summary.getKey());
             return summary.getKey();
           }).forEach(key -> waitForResult("Delete s3: " + s3BucketName + "/" + key, () -> {
      try {
        return s3.getObjectMetadata(s3BucketName, key);
      } catch (AmazonS3Exception e) {
        return null;
      }
    }, m -> m == null));


    // firehose
    AmazonKinesisFirehoseClient firehoseClient =
      new AmazonKinesisFirehoseClient(awsCredentials.getProvider());
    firehoses.stream().forEach(stream -> {
      DeleteDeliveryStreamRequest delete = new DeleteDeliveryStreamRequest()
        .withDeliveryStreamName(stream);
      firehoseClient.deleteDeliveryStream(delete);
      waitForResult("Ensuring Firehose delete of: " + stream, () -> {
        try {
          return firehoseClient.describeDeliveryStream(new DescribeDeliveryStreamRequest()
            .withDeliveryStreamName(stream));
        } catch (com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException e) {
          return null;
        }
      }, result -> result != null);
    });

    // kinesis
    AmazonKinesisClient client = getKinesis();
    kinesisStreams.stream().forEach(name -> {
      DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
      deleteStreamRequest.setStreamName(name);
      client.deleteStream(deleteStreamRequest);

      waitForResult("Ensure delete of kinesis stream: " + name, () -> {
        try {
          return client.describeStream(name);
        } catch (com.amazonaws.services.kinesis.model.ResourceNotFoundException e) {
          return null;
        }
      }, description -> description == null);
    });
  }

  private void setupAws(TestProgress progress) throws Exception {
    List<ListenableFuture<?>> futures = new ArrayList<>();
    futures.add(executor.submit(() -> {
      // setup the schema store
      Map<String, Object> json = progress.event;
      SchemaStore store = props.createSchemaStore();
      progress.store = store;
      LambdaTestUtils.updateSchemaStore(store, json);
      LOG.debug("Schema store setup complete!");
      return null;
    }));

    // setup the firehose connections
    futures.add(executor.submit(() -> {
      String archive = props.getFirehoseStream(LambdaClientProperties.RAW_PREFIX,
        LambdaClientProperties.StreamType.ARCHIVE);
      progress.firehoseArchive = archive;
      createFirehose(archive, props.getTestPrefix());
      LOG.debug("Firehose setup complete!");
      return null;
    }));

    // setup the kinesis streams
    futures.add(executor.submit(() -> {
      setupKinesis();
      LOG.debug("Kinesis setup complete!");
      return null;
    }));

    CountDownLatch latch = new CountDownLatch(futures.size());
    for (ListenableFuture f : futures) {
      Futures.addCallback(f, new FutureCallback() {
        @Override
        public void onSuccess(Object result) {
          latch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
          latch.countDown();
        }
      });
    }
    // wait for all the setup to complete
    latch.await();
  }

  private AmazonKinesisClient getKinesis() {
    AmazonKinesisClient client = new AmazonKinesisClient(awsCredentials.getProvider());
    client.setRegion(RegionUtils.getRegion(region));
    return client;
  }

  private void setupKinesis() {
    AmazonKinesisClient client = getKinesis();
    CreateStreamRequest createStreamRequest = new CreateStreamRequest();
    String stream = props.getRawToStagedKinesisStreamName();
    kinesisStreams.add(stream);
    createStreamRequest.setStreamName(stream);
    createStreamRequest.setShardCount(kinesisShardCount);
    client.createStream(createStreamRequest);
    waitForResult("Create Kinesis stream:" + stream, () -> {
      DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
      describeStreamRequest.setStreamName(stream);
      DescribeStreamResult describeStreamResponse =
        client.describeStream(describeStreamRequest);
      return describeStreamResponse.getStreamDescription().getStreamStatus();
    }, streamStatus -> streamStatus.equals("ACTIVE"));
  }

  private void createFirehose(String stream, String s3Prefix) {
    firehoses.add(stream);
    AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(awsCredentials
      .getProvider());
    CreateDeliveryStreamRequest create = new CreateDeliveryStreamRequest();
    create.setDeliveryStreamName(stream);

    LOG.debug("Creating firehose [" + stream + "] writing to " + s3BucketArn + "/" + s3Prefix);
    S3DestinationConfiguration destConf = new S3DestinationConfiguration();
    destConf.setBucketARN(s3BucketArn);
    destConf.setPrefix(s3Prefix);
    destConf.setBufferingHints(new BufferingHints()
      .withIntervalInSeconds(60)
      .withSizeInMBs(1));
    // Could also specify GZIP, ZIP, or SNAPPY
    destConf.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
    destConf.setRoleARN(firehoseToS3RoleArn);
    create.setS3DestinationConfiguration(destConf);
    firehoseClient.createDeliveryStream(create);

    // wait for the stream to be ready, but don't fail if it isn't
    waitForResult("Create Firehose stream: " + stream, () -> {
      DescribeDeliveryStreamRequest describe = new DescribeDeliveryStreamRequest()
        .withDeliveryStreamName(stream);
      DescribeDeliveryStreamResult result = firehoseClient.describeDeliveryStream(describe);
      return result.getDeliveryStreamDescription().getDeliveryStreamStatus();
    }, status -> status.equals("ACTIVE"));
  }

  private void makeRequest(TestProgress progress) throws IOException, OldSchemaException {
    LOG.info("------ > Making request ----->");
    Map<String, Object> json = progress.event;
    ByteBuffer bytes = LambdaTestUtils.asBytes(json);
    progress.rawData = bytes.array();
    String data = Base64.getEncoder().encodeToString(Arrays.copyOfRange(bytes.array(), bytes
      .arrayOffset(), bytes.remaining()));
    LOG.info("With data: " + data);

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

  private <RESULT> boolean waitForResult(String description, Supplier<RESULT> status,
    Predicate<RESULT> statusCheck) {
    try {
      return waitForResult(description, status, statusCheck, THREE_HUNDRED_SECONDS, ONE_SECOND);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private <RESULT> boolean waitForResult(String description, Supplier<RESULT> status,
    Predicate<RESULT> statusCheck, int timeoutMs, int intervalMs) throws InterruptedException {
    Preconditions.checkArgument(timeoutMs > 0, "Timeout must be >= 0");
    if (intervalMs > 0 && intervalMs < timeoutMs) {
      LOG.info(
        "Waiting for [" + description + "]. Max wait: " + timeoutMs / 1000 + "s");
      long startTime = System.currentTimeMillis();
      long endTime = startTime + (long) timeoutMs;
      for (; System.currentTimeMillis() < endTime; Thread.sleep((long) intervalMs)) {
        try {
          if (statusCheck.test(status.get())) {
            return true;
          }
        } catch (ResourceNotFoundException var11) {
        }
      }
      LOG.warn(String.format("Resource [%s] didn't not become active/created within %d sec!",
        description, timeoutMs / 1000));
      return false;
    } else {
      throw new IllegalArgumentException("Interval must be > 0 and < timeout");
    }
  }

  private void validate(TestProgress progress) throws IOException, InterruptedException {
    // read the data from S3 to ensure it matches the raw data sent
    AmazonS3 s3 = new AmazonS3Client(awsCredentials.getProvider());
    ObjectListing[] lists = new ObjectListing[1];
    int count[] = new int[1];
    assertTrue("Didn't get a record under " + s3BucketName + ":" + props
        .getTestPrefix() + " after " + THREE_HUNDRED_SECONDS / 1000 + "s",
      waitForResult("Firehose -> s3 write", () -> {
        ObjectListing listing = s3.listObjects(s3BucketName, props.getTestPrefix());
        lists[0] = listing;
        return listing.getObjectSummaries();
      }, summaries -> {
        if (count[0]++ % 10 == 0) {
          LOG.info("Got objects: " + summaries);
        }
        return summaries.size() > 0;
      }));

    // read the most recent record from the bucket with our prefix
    ObjectListing listing = lists[0];
    List<String> objects = new ArrayList<>();
    Optional<S3ObjectSummary> optionalSummary =
      listing.getObjectSummaries().stream().sorted((s1, s2) -> {
        ZonedDateTime time = parseFromS3ObjectName(s1.getKey(), progress
          .firehoseArchive);
        ZonedDateTime t2 = parseFromS3ObjectName(s2.getKey(), progress
          .firehoseArchive);
        return time.compareTo(t2);
      }).peek(summary -> objects.add(summary.getKey())).findFirst();
    assertTrue("Didn't find a matching summary from " + listing, optionalSummary.isPresent());

    // read the object
    S3ObjectSummary summary = optionalSummary.get();
    S3Object object = s3.getObject(new GetObjectRequest(s3BucketName, summary.getKey()));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copy(object.getObjectContent(), out);
    String expected = new String(progress.rawData);
    String actual = new String(out.toByteArray());
    assertArrayEquals(
      "Raw data sent\n[" + expected + "]\n and s3 content\n[" + actual + "] don't match in "
      + "object: " + summary.getKey() + "\nof potential objects: " + objects,
      progress.rawData, out.toByteArray());

    // read the event from Kinesis to ensure it parses the record we expect
    AmazonKinesisClient kinesis = getKinesis();
    GetShardIteratorResult shard =
      kinesis.getShardIterator(props.getRawToStagedKinesisStreamName(), "0", "TRIM_HORIZON");
    String iterator = shard.getShardIterator();
    GetRecordsResult getResult = kinesis.getRecords(new GetRecordsRequest().withShardIterator
      (iterator).withLimit(1));
    List<Record> records = getResult.getRecords();
    assertEquals(1, records.size());
    Record record = records.get(0);
    FirehoseRecordReader<GenericRecord> recordReader =
      FirehoseRecordReader.create(record.getData());
    GenericRecord avro = recordReader.next();
    EndToEndTestUtil.verifyRecordMatchesJson(progress.store, progress.event, avro);
  }

  private ZonedDateTime parseFromS3ObjectName(String name, String stream) {
    name = name.substring(name.lastIndexOf('/') + 1);
    name = name.replaceFirst(stream, "");
    name = name.replaceFirst("-1-", "");
    int[] i = Stream.of(name.split("-")).limit(6).mapToInt(s -> Integer.valueOf(s))
                    .toArray();
    return ZonedDateTime.of(LocalDateTime.of(i[0], i[1], i[2], i[3], i[4], i[5]), ZoneId.of("Z"));
  }


  private class TestProgress {
    private Map<String, Object> event;
    private byte[] rawData;
    public SchemaStore store;
    public String firehoseArchive;
  }
}