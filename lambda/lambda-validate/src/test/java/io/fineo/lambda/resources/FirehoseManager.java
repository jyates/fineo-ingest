package io.fineo.lambda.resources;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DeleteDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.S3DestinationConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.TestProperties;
import io.fineo.lambda.util.FutureWaiter;
import io.fineo.lambda.util.Join;
import io.fineo.lambda.util.ResultWaiter;
import io.fineo.schema.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Manage firehose connection and result lookup
 */
public class FirehoseManager {
  private static final Log LOG = LogFactory.getLog(FirehoseManager.class);
  public static final int READ_S3_INTERVAL = TestProperties.ONE_SECOND * 10;

  private final LambdaClientProperties props;
  private final AWSCredentialsProvider provider;
  private final AmazonS3Client s3;
  private Map<String, Boolean> prefixes = new HashMap<>();
  private Map<String, String> firehoseNameToStage = new HashMap<>();
  private Map<String, String> firehosesToS3 = new ConcurrentHashMap<>();
  private Map<String, List<String>> stageToS3Prefix = new HashMap<>();

  public FirehoseManager(LambdaClientProperties props, AWSCredentialsProvider provider) {
    this.provider = provider;
    this.props = props;
    this.s3 = new AmazonS3Client(provider);
  }

  public void createFirehoses(String stage, FutureWaiter future) {
    this.prefixes.put(stage, false);
    // create the firehoses
    stageToS3Prefix.put(stage, s3Tracker());
    for (LambdaClientProperties.StreamType type : LambdaClientProperties.StreamType.values()) {
      future.run(() -> stageToS3Prefix.get(stage).add(createFirehose(stage, type)));
    }
  }

  private List<String> s3Tracker() {
    return Collections.synchronizedList(new ArrayList<>(3));
  }

  private String createFirehose(String stage, LambdaClientProperties.StreamType type) {
    String stream = props.getFirehoseStreamName(stage, type);
    firehoseNameToStage.put(stream, stage);
    AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(provider);
    CreateDeliveryStreamRequest create = new CreateDeliveryStreamRequest();
    create.setDeliveryStreamName(stream);

    String prefix = props.getTestPrefix() + stream + "/";
    firehosesToS3.put(stream, prefix);
    LOG.debug(
      "Creating firehose [" + stream + "] writing to " + TestProperties.Firehose.S3_BUCKET_ARN +
      "/" + prefix);
    S3DestinationConfiguration destConf = new S3DestinationConfiguration();
    destConf.setBucketARN(TestProperties.Firehose.S3_BUCKET_ARN);
    destConf.setPrefix(prefix);
    destConf.setBufferingHints(new BufferingHints()
      .withIntervalInSeconds(60)
      .withSizeInMBs(1));
    // Could also specify GZIP, ZIP, or SNAPPY
    destConf.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
    destConf.setRoleARN(TestProperties.Firehose.FIREHOSE_TO_S3_ARN_ROLE);
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

  public List<ByteBuffer> read(String streamName) {
    long timeout = getTimeout(streamName);

    // read the data from S3 to ensure it matches the raw data sent
    String prefix = firehosesToS3.get(streamName);

    // the first time we wait on S3, we want to wait a good amount of time to ensure firehoses
    // flushed to the S3. After that point, we should have no more waiting.
    ResultWaiter<ObjectListing> wait = new ResultWaiter<>()
      .withInterval(READ_S3_INTERVAL)
      .withTimeout(Math.min(timeout, READ_S3_INTERVAL))
      .withDescription(
        "Firehose -> s3 [" + TestProperties.Firehose.S3_BUCKET_NAME + "/" + prefix
        + "] write; "
        + "expected: ~60sec")
      .withStatus(() -> s3.listObjects(TestProperties.Firehose.S3_BUCKET_NAME, prefix))
      .withStatusCheck(
        listing -> ((ObjectListing) listing).getObjectSummaries().size() > 0);
    if (!wait.waitForResult()) {
      return new ArrayList<>(0);
    }

    // read the most recent record from the bucket with our prefix
    ObjectListing listing = wait.getLastStatus();
    List<String> objects = new ArrayList<>();
    Optional<S3ObjectSummary> optionalSummary =
      listing.getObjectSummaries().stream().sorted((s1, s2) -> {
        ZonedDateTime time = parseTimeFromS3ObjectName(s1.getKey(), streamName);
        ZonedDateTime t2 = parseTimeFromS3ObjectName(s2.getKey(), streamName);
        // descending order
        return -time.compareTo(t2);
      }).peek(summary -> objects.add(summary.getKey())).findFirst();
    if (!optionalSummary.isPresent()) {
      return new ArrayList<>(0);
    }

    // read the object
    S3ObjectSummary summary = optionalSummary.get();
    S3Object object =
      s3.getObject(new GetObjectRequest(TestProperties.Firehose.S3_BUCKET_NAME, summary.getKey()));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      IOUtils.copy(object.getObjectContent(), out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Lists.newArrayList(ByteBuffer.wrap(out.toByteArray()));
  }

  /**
   * Determine what timeout we should used based on the stage. Each stage should be waited
   * independently, since records may have not made it to the next stage.
   * <p>
   * Firehose takes a minute to flush results to s3. We wait doublee that, just in case, the
   * first time, but after that all writes should have been flushed, so we just read it
   * immediately, for each stage
   * </p>
   */
  private long getTimeout(String streamName) {
    String prefix = this.firehoseNameToStage.get(streamName);
    long timeout = 2 * TestProperties.ONE_MINUTE;
    if (prefixes.get(prefix)) {
      timeout = TestProperties.ONE_SECOND * 2;
    }
    prefixes.put(prefix, true);
    return timeout;
  }

  private ZonedDateTime parseTimeFromS3ObjectName(String name, String stream) {
    name = name.substring(name.lastIndexOf('/') + 1);
    name = name.replaceFirst(stream, "");
    name = name.replaceFirst("-1-", "");
    int[] i = Stream.of(name.split("-")).limit(6).mapToInt(Integer::valueOf).toArray();
    return ZonedDateTime.of(LocalDateTime.of(i[0], i[1], i[2], i[3], i[4], i[5]), ZoneId.of("Z"));
  }

  public void cleanupFirehoses(FutureWaiter futures) {
    // firehose
    AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(provider);
    firehosesToS3.keySet().stream().forEach(stream -> futures.run(() -> {
      DeleteDeliveryStreamRequest delete = new DeleteDeliveryStreamRequest()
        .withDeliveryStreamName(stream);
      ResultWaiter.doOrNull(() -> firehoseClient.deleteDeliveryStream(delete));
      new ResultWaiter<>()
        .withDescription("Ensuring Firehose delete of: " + stream)
        .withStatusNull(
          () -> firehoseClient.describeDeliveryStream(new DescribeDeliveryStreamRequest()
            .withDeliveryStreamName(stream)))
        .waitForResult();
    }));
  }

  public void cleanupData() {
    // remove all s3 files with the current test prefix
    S3Delete delete = new S3Delete(provider).withBucket(TestProperties.Firehose.S3_BUCKET_NAME);
    delete.delete(props.getTestPrefix());
  }

  public void ensureNoDataStored() {
    List<S3ObjectSummary> objs =
      s3.listObjects(TestProperties.Firehose.S3_BUCKET_NAME, props.getTestPrefix())
        .getObjectSummaries();
    assertEquals("There were some s3 files created! Files: " + Joiner.on(',').join(
      objs.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList())), 0, objs.size());
  }

  public void cleanupData(List<Pair<String, LambdaClientProperties.StreamType>> streams) {
    if (streams.size() == 0) {
      return;
    }
    S3Delete delete = new S3Delete(provider).withBucket(TestProperties.Firehose.S3_BUCKET_NAME);
    for (Pair<String, LambdaClientProperties.StreamType> stream : streams) {
      String name = props.getFirehoseStreamName(stream.getKey(), stream.getValue());
      String path = firehosesToS3.remove(name);
      LOG.debug("DELETE:" + name + " -> " + path);
      delete.delete(path);
    }
    String output =
      firehosesToS3.entrySet().stream()
                   .map(entry -> entry.getKey() + " -> " + entry.getValue())
                   .reduce(null, Join.on(",\n"));
    LOG.error("There were some S3 locations that were not deleted because they contain failed "
              + "test information!\nS3 Locations: " + output);
  }

}
