package io.fineo.lambda;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.avro.AvroDynamoReader;
import io.fineo.lambda.util.EndToEndTestRunner;
import io.fineo.lambda.util.EndtoEndSuccessStatus;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Manages lambda test resources on AWS
 */
public class AwsResourceManager implements ResourceManager {
  private static final Log LOG = LogFactory.getLog(AwsResourceManager.class);
  public static final int ONE_SECOND = 1000;
  public static final int ONE_MINUTE = ONE_SECOND * 60;
  public static final int THREE_HUNDRED_SECONDS = 3 * 100 * ONE_SECOND;
  private static final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
    MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));
  private static final String RAW_TO_AVRO_ARN =
    "arn:aws:lambda:us-east-1:766732214526:function:RawToAvro";

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

  /**
   * test role for the Firehose stream -> S3 bucket
   */
  private String firehoseToS3RoleArn = "arn:aws:iam::766732214526:role/test-lambda-functions";
  private String s3BucketName = "test.fineo.io";
  private String s3BucketArn = "arn:aws:s3:::" + s3BucketName;
  private Map<String, String> firehosesToS3 = new ConcurrentHashMap<>();
  private Integer kinesisShardCount = 1;
  private List<String> kinesisStreams = Collections.synchronizedList(new ArrayList<>());

  private final String region = System.getProperty("aws-region", "us-east-1");
  private final AwsCredentialResource awsCredentials;
  private LambdaClientProperties props;
  private SchemaStore store;
  private List<String> rawToAvroS3 = new ArrayList<>(3);
  private List<String> avroToStoreS3 = new ArrayList<>(3);
  private boolean firstS3FlushWait = true;

  public AwsResourceManager(AwsCredentialResource awsCredentials) {
    this.awsCredentials = awsCredentials;
  }

  @Override
  public void setup(LambdaClientProperties props) throws Exception {
    this.props = props;
    SchemaStore[] storeRef = new SchemaStore[1];
    FutureWaiter future = new FutureWaiter();
    future.run(() -> {
      SchemaStore store = props.createSchemaStore();
      storeRef[0] = store;
      LOG.debug("Schema store creation complete!");
    });

    // setup the firehose connections
    Map<String, List<String>> s3 = new HashMap<>();
    s3.put(LambdaClientProperties.RAW_PREFIX, rawToAvroS3);
    s3.put(LambdaClientProperties.STAGED_PREFIX, avroToStoreS3);
    for (String s : s3.keySet()) {
      for (LambdaClientProperties.StreamType type : LambdaClientProperties.StreamType.values()) {
        future.run(() -> {
          s3.get(s).add(createFirehose(s, type));
        });
      }
    }

    // setup the kinesis streams
    future.run(() -> {
      setupKinesis();
      LOG.debug("Kinesis setup complete!");
    });
    // wait for all the setup to complete
    future.await();
    this.store = storeRef[0];
  }

  private String createFirehose(String stage, LambdaClientProperties.StreamType type) {
    String archive = props.getFirehoseStreamName(stage, type);
    return createFirehose(archive, props.getTestPrefix());
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
    new ResultWaiter<>()
      .withDescription("Create Kinesis stream:" + stream)
      .withStatus(() -> {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(stream);
        DescribeStreamResult describeStreamResponse =
          client.describeStream(describeStreamRequest);
        return describeStreamResponse.getStreamDescription().getStreamStatus();
      }).withStatusCheck(streamStatus -> streamStatus.equals("ACTIVE"))
      .waitForResult();
  }

  /**
   * Create a firehose with the given stream name and with the specified s3 prefix provided
   *
   * @param stream
   * @param s3Prefix
   * @return the full s3 prefix for the stream
   */
  private String createFirehose(String stream, String s3Prefix) {
    AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(awsCredentials
      .getProvider());
    CreateDeliveryStreamRequest create = new CreateDeliveryStreamRequest();
    create.setDeliveryStreamName(stream);

    String prefix = stream + "/" + s3Prefix;
    firehosesToS3.put(stream, prefix);
    LOG.debug("Creating firehose [" + stream + "] writing to " + s3BucketArn + "/" + prefix);
    S3DestinationConfiguration destConf = new S3DestinationConfiguration();
    destConf.setBucketARN(s3BucketArn);
    destConf.setPrefix(prefix);
    destConf.setBufferingHints(new BufferingHints()
      .withIntervalInSeconds(60)
      .withSizeInMBs(1));
    // Could also specify GZIP, ZIP, or SNAPPY
    destConf.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
    destConf.setRoleARN(firehoseToS3RoleArn);
    create.setS3DestinationConfiguration(destConf);
    firehoseClient.createDeliveryStream(create);

    // wait for the stream to be ready, but don't fail if it isn't
    new ResultWaiter<>()
      .withDescription("Create Firehose stream: " + stream)
      .withStatus(() -> {
        DescribeDeliveryStreamRequest describe = new DescribeDeliveryStreamRequest()
          .withDeliveryStreamName(stream);
        DescribeDeliveryStreamResult result = firehoseClient.describeDeliveryStream(describe);
        return result.getDeliveryStreamDescription().getDeliveryStreamStatus();
      }).withStatusCheck(status -> status.equals("ACTIVE"))
      .waitForResult();
    return prefix;
  }


  @Override
  public byte[] send(Map<String, Object> json) throws Exception {
    LOG.info("------ > Making request ----->");
    byte[] array = LambdaTestUtils.asBytes(json);
    String data = Base64.getEncoder().encodeToString(array);
    LOG.info("With data: " + data);

    InvokeRequest request = new InvokeRequest();
    request.setFunctionName(RAW_TO_AVRO_ARN);
    request.withPayload(String.format(TEST_EVENT, data, region));
    request.setInvocationType(InvocationType.RequestResponse);
    request.setLogType(LogType.Tail);

    AWSLambdaClient client = new AWSLambdaClient(awsCredentials.getProvider());
    InvokeResult result = client.invoke(request);
    LOG.info("Status: " + result.getStatusCode());
    LOG.info("Error:" + result.getFunctionError());
    LOG.info("Log:" + new String(Base64.getDecoder().decode(result.getLogResult())));
    assertNull(result.getFunctionError());
    return array;
  }

  @Override
  public List<ByteBuffer> getFirhoseWrites(String stream) {
    // read the data from S3 to ensure it matches the raw data sent
    AmazonS3 s3 = new AmazonS3Client(awsCredentials.getProvider());
    String prefix = firehosesToS3.get(stream);

    // the first time we wait on S3, we want to wait a good amount of time to ensure firehoses
    // flushed to the S3. After that point, we should have no more waiting.
    Supplier<ObjectListing> lister = () -> s3.listObjects(s3BucketName, prefix);
    ObjectListing[] lists = new ObjectListing[1];
    int count[] = new int[1];

    boolean records = new ResultWaiter<>()
      // firehose takes a minute to flush results to s3. We wait doule that, just incase, the
      // first time, but after that all writes should have been flushed, so we just read it
      // immediately
      .withTimeout(firstS3FlushWait ? 2 * ONE_MINUTE : ONE_SECOND + 1)
      .withDescription("Firehose -> s3 [" + (s3BucketName + prefix) + "] write; expected: ~60sec")
      .withStatus(() -> {
        ObjectListing listing = lister.get();
        lists[0] = listing;
        return listing.getObjectSummaries();
      }).withStatusCheck(summaries -> {
        if (count[0]++ % 10 == 0) {
          LOG.info("Got objects: " + summaries);
        }
        return ((List<S3ObjectSummary>) summaries).size() > 0;
      })
      .waitForResult();
    firstS3FlushWait = false;
    if (!records) {
      return new ArrayList<>(0);
    }

    // read the most recent record from the bucket with our prefix
    ObjectListing listing = lists[0];
    List<String> objects = new ArrayList<>();
    Optional<S3ObjectSummary> optionalSummary =
      listing.getObjectSummaries().stream().sorted((s1, s2) -> {
        ZonedDateTime time = parseFromS3ObjectName(s1.getKey(), stream);
        ZonedDateTime t2 = parseFromS3ObjectName(s2.getKey(), stream);
        return time.compareTo(t2);
      }).peek(summary -> objects.add(summary.getKey())).findFirst();
    if (!optionalSummary.isPresent()) {
      return new ArrayList<>(0);
    }

    // read the object
    S3ObjectSummary summary = optionalSummary.get();
    S3Object object = s3.getObject(new GetObjectRequest(s3BucketName, summary.getKey()));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      IOUtils.copy(object.getObjectContent(), out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Lists.newArrayList(ByteBuffer.wrap(out.toByteArray()));
  }

  private ZonedDateTime parseFromS3ObjectName(String name, String stream) {
    name = name.substring(name.lastIndexOf('/') + 1);
    name = name.replaceFirst(stream, "");
    name = name.replaceFirst("-1-", "");
    int[] i = Stream.of(name.split("-")).limit(6).mapToInt(s -> Integer.valueOf(s))
                    .toArray();
    return ZonedDateTime.of(LocalDateTime.of(i[0], i[1], i[2], i[3], i[4], i[5]), ZoneId.of("Z"));
  }

  @Override
  public List<ByteBuffer> getKinesisWrites(String stream) {
    AmazonKinesisClient kinesis = getKinesis();
    GetShardIteratorResult shard =
      kinesis.getShardIterator(stream, "0", "TRIM_HORIZON");
    String iterator = shard.getShardIterator();
    GetRecordsResult getResult = kinesis.getRecords(new GetRecordsRequest().withShardIterator
      (iterator));
    return getResult.getRecords().stream().map(record -> record.getData()).collect(Collectors
      .toList());
  }

  @Override
  public SchemaStore getStore() {
    return this.store;
  }

  @Override
  public void verifyDynamoWrites(RecordMetadata metadata, Map<String, Object> json) {
    String tablePrefix = props.getDynamoIngestTablePrefix();
    AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(awsCredentials.getProvider());
    ListTablesResult tablesResult = client.listTables(tablePrefix);
    assertEquals(
      "Wrong number of tables after prefix" + tablePrefix + ". Got tables: " + tablesResult
        .getTableNames(), 1, tablesResult.getTableNames().size());

    AvroDynamoReader reader = new AvroDynamoReader(getStore(), client, tablePrefix);
    Metric metric = getStore().getMetricMetadata(metadata);
    long ts = metadata.getBaseFields().getTimestamp();
    Range<Instant> range = Range.of(ts, ts + 1);
    List<GenericRecord> records =
      reader.scan(metadata.getOrgID(), metric, range, null).collect(Collectors.toList());
    assertEquals(Lists.newArrayList(records.get(0)), records.size());
    EndToEndTestRunner.verifyRecordMatchesJson(getStore(), json, records.get(0));
  }

  @Override
  public void cleanup(EndtoEndSuccessStatus status) throws InterruptedException {
    cleanupBasicResources();
    if (status == null) {
      return;
    }

    if (status.isSuccessful()) {
      cleanupAllS3();
      return;
    }

    cleanupNonErrorS3(status);

    cleanupNonErrorDynamo(status);
  }

  private void cleanupNonErrorDynamo(EndtoEndSuccessStatus status) {
    String table = props.getDynamoIngestTablePrefix();
    if (!status.isAvroToStorageSuccessful()) {
      LOG.info("Dynamo table(s) starting with " + table + " was not deleted because there was an "
               + "error validating dynamo");
    }
    deleteDynamoTables(table);
  }

  private void cleanupNonErrorS3(EndtoEndSuccessStatus status) {
    if (!status.isMessageSent() || !status.isUpdateStoreCorrect()) {
      Stream<S3ObjectSummary> objs = getAllS3Files(s3());
      assertEquals("There were some s3 files created! Files: " +
                   Joiner.on(',').join(objs.map(obj -> obj.getKey()).collect(Collectors.toList())),
        0, objs.count());
      return;
    }

    // only cleanup the S3 files that we don't need to keep track of because that phase was
    // successful
    List<String> ignoredPrefixes = Lists.newArrayList(firehosesToS3.values());
    if (status.isRawToAvroSuccessful()) {
      for (String path : rawToAvroS3) {
        ignoredPrefixes.remove(path);
        delete(s3(), path);
      }
    } else {
      // check each stage
      ignoredPrefixes.remove(deleteFirehoseS3BucketIfSuccessful(LambdaClientProperties.RAW_PREFIX,
        LambdaClientProperties.StreamType.ARCHIVE, status));
      ignoredPrefixes.remove(deleteFirehoseS3BucketIfSuccessful(LambdaClientProperties.RAW_PREFIX,
        LambdaClientProperties.StreamType.PROCESSING_ERROR, status));
      ignoredPrefixes.remove(deleteFirehoseS3BucketIfSuccessful(LambdaClientProperties.RAW_PREFIX,
        LambdaClientProperties.StreamType.COMMIT_ERROR, status));
    }

    if (status.isAvroToStorageSuccessful()) {
      for (String path : avroToStoreS3) {
        ignoredPrefixes.remove(path);
        delete(s3(), path);
      }
    } else {
      ignoredPrefixes
        .remove(deleteFirehoseS3BucketIfSuccessful(LambdaClientProperties.STAGED_PREFIX,
          LambdaClientProperties.StreamType.ARCHIVE, status));
      ignoredPrefixes
        .remove(deleteFirehoseS3BucketIfSuccessful(LambdaClientProperties.STAGED_PREFIX,
          LambdaClientProperties.StreamType.PROCESSING_ERROR, status));
      ignoredPrefixes
        .remove(deleteFirehoseS3BucketIfSuccessful(LambdaClientProperties.STAGED_PREFIX,
          LambdaClientProperties.StreamType.COMMIT_ERROR, status));
    }

    LOG.error("There were some S3 locations that were not deleted because they contain failed "
              + "test information! Locations: " + ignoredPrefixes.stream().map(p ->
      s3BucketName + "/" + p).reduce("", (result, next) -> result + ",\n"));
  }

  /**
   * Delete the s3 path associated with the prefix and {@link io.fineo.lambda
   * .LambdaClientProperties.StreamType}, if that stream was marked as successful.
   *
   * @param prefix
   * @param type
   * @param status
   * @return the delete path, or <tt>null</tt> if nothing was deleted
   */
  private String deleteFirehoseS3BucketIfSuccessful(String prefix, LambdaClientProperties
    .StreamType type, EndtoEndSuccessStatus status) {
    String firehose = props.getFirehoseStreamName(prefix, type);

    if (!status.getCorrectFirehoses().contains(firehose)) {
      return null;
    }

    // delete the path because it was successful
    String path = firehosesToS3.get(firehose);
    delete(s3(), path);
    return path;
  }

  private AmazonS3Client s3() {
    return new AmazonS3Client(awsCredentials.getProvider());
  }

  private void cleanupAllS3() {
    // remove all s3 files with the current test prefix
    AmazonS3 s3 = s3();
    getAllS3Files(s3)
      .peek(s -> LOG.info("Deleting " + s3BucketName + "/" + s.getKey()))
      .map(summary -> {
        s3.deleteObject(s3BucketName, summary.getKey());
        return summary.getKey();
      }).forEach(key -> delete(s3(), key));
  }

  private void delete(AmazonS3Client s3, String key) {
    new ResultWaiter<>()
      .withDescription("Delete s3: " + s3BucketName + "/" + key)
      .withStatusNull(() -> s3
        .getObjectMetadata(s3BucketName, key))
      .waitForResult();
  }

  private Stream<S3ObjectSummary> getAllS3Files(AmazonS3 s3) {
    return s3.listObjects(s3BucketName, props.getTestPrefix()).getObjectSummaries().stream();
  }

  private void cleanupBasicResources() throws InterruptedException {
    FutureWaiter futures = new FutureWaiter();
    //dynamo
    futures.run(() -> deleteDynamoTables(props.getSchemaStoreTable()));

    // firehose
    AmazonKinesisFirehoseClient firehoseClient =
      new AmazonKinesisFirehoseClient(awsCredentials.getProvider());
    firehosesToS3.keySet().stream().forEach(stream -> {
      futures.run(() -> {
        DeleteDeliveryStreamRequest delete = new DeleteDeliveryStreamRequest()
          .withDeliveryStreamName(stream);
        ResultWaiter.doOrNull(() -> firehoseClient.deleteDeliveryStream(delete));
        new ResultWaiter<>()
          .withDescription("Ensuring Firehose delete of: " + stream)
          .withStatusNull(
            () -> firehoseClient.describeDeliveryStream(new DescribeDeliveryStreamRequest()
              .withDeliveryStreamName(stream)))
          .waitForResult();
      });
    });

    // kinesis
    AmazonKinesisClient client = getKinesis();
    kinesisStreams.stream().forEach(name -> {
      futures.run(() -> {
        DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
        deleteStreamRequest.setStreamName(name);
        client.deleteStream(deleteStreamRequest);

        new ResultWaiter<>()
          .withDescription("Ensure delete of kinesis stream: " + name)
          .withStatusNull(() -> client.describeStream(name))
          .waitForResult();
      });
    });
    futures.await();
  }

  private class FutureWaiter {
    private List<ListenableFuture> futures = new ArrayList<>();

    public void run(Runnable r) {
      ListenableFuture future = executor.submit(r);
      this.futures.add(future);
    }

    public void await() throws InterruptedException {
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
      latch.await();
    }
  }

  private void deleteDynamoTables(String tableNamesPrefix) {
    // dynamo
    ListTablesResult tables = props.getDynamo().listTables(props.getTestPrefix());
    for (String name : tables.getTableNames()) {
      if (!name.startsWith(props.getTestPrefix())) {
        LOG.debug("Stopping deletion of dynamo tables with name: " + name);
        break;
      }
      if (name.startsWith(tableNamesPrefix)) {
        AmazonDynamoDBAsyncClient dynamo = props.getDynamo();
        dynamo.deleteTable(name);
        new ResultWaiter<>()
          .withDescription("Deletion dynamo table: " + name)
          .withStatusNull(() -> dynamo.describeTable(name))
          .waitForResult();
      }
    }
  }
}