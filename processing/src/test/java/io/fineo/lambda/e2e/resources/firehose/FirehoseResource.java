package io.fineo.lambda.e2e.resources.firehose;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DeleteDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.ResourceInUseException;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
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
import io.fineo.lambda.e2e.resources.AwsResource;
import io.fineo.lambda.e2e.resources.ResourceUtils;
import io.fineo.lambda.e2e.resources.S3Resource;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Manage firehose connection and result lookup
 */
public class FirehoseResource implements AwsResource {
  private static final Log LOG = LogFactory.getLog(FirehoseResource.class);
  public static final long READ_S3_INTERVAL = TestProperties.ONE_SECOND * 10;

  private final LambdaClientProperties props;
  private final AWSCredentialsProvider provider;
  private final AmazonS3Client s3;
  private final AmazonKinesisFirehoseClient firehoseClient;
  private final ResultWaiter.ResultWaiterFactory waiter;
  private FirehoseStreams streams = new FirehoseStreams();

  public FirehoseResource(LambdaClientProperties props, AWSCredentialsProvider provider,
    ResultWaiter.ResultWaiterFactory waiter) {
    this.provider = provider;
    this.props = props;
    this.waiter = waiter;
    this.s3 = new AmazonS3Client(provider);
    firehoseClient = new AmazonKinesisFirehoseClient(provider);
  }

  public void createFirehoses(String stage, FutureWaiter future) {
    for (LambdaClientProperties.StreamType type : LambdaClientProperties.StreamType.values()) {
      future.run(() -> createFirehose(stage, type));
    }
  }

  private void createFirehose(String stage, LambdaClientProperties.StreamType type) {
    String stream = props.getFirehoseStreamName(stage, type);
    String s3Path = stream + "/";

    // track the firehose information
    Pair<String, LambdaClientProperties.StreamType> key = new Pair<>(stage, type);
    streams.store(key, stream, s3Path);

    // create the stream, if it doesn't exist already
    if (exists(stream)) {
      LOG.debug("Skipping checking for stream active => someone else is already doing it (hint: "
                + "the guy who created the stream");
      return;
    }
    LOG.debug(
      "Creating firehose [" + stream + "] -> " + TestProperties.Firehose.S3_BUCKET_ARN +
      "/" + s3Path);
    CreateDeliveryStreamRequest create = new CreateDeliveryStreamRequest();
    create.setDeliveryStreamName(stream);
    S3DestinationConfiguration destConf = new S3DestinationConfiguration();
    destConf.setBucketARN(TestProperties.Firehose.S3_BUCKET_ARN);
    destConf.setPrefix(s3Path);
    destConf.setBufferingHints(new BufferingHints()
      .withIntervalInSeconds(60)
      .withSizeInMBs(1));
    // Could also specify GZIP, ZIP, or SNAPPY
    destConf.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
    destConf.setRoleARN(TestProperties.Firehose.FIREHOSE_TO_S3_ARN_ROLE);
    create.setS3DestinationConfiguration(destConf);
    try {
      firehoseClient.createDeliveryStream(create);
    } catch (ResourceInUseException e) {
      LOG.debug(stream + " appears to already exist: " + e.getMessage());
      return;
    }
    LOG.info(stage + ", " + type + " -> " + stream + " created!");

    // wait for the stream to be ready, but don't fail if it isn't
    waiter.get()
          .withDescription("Firehose stream: " + stream + " activation")
          .withStatus(() -> {
            DescribeDeliveryStreamRequest describe = new DescribeDeliveryStreamRequest()
              .withDeliveryStreamName(stream);
            DescribeDeliveryStreamResult result = firehoseClient.describeDeliveryStream(describe);
            return result.getDeliveryStreamDescription().getDeliveryStreamStatus();
          }).withStatusCheck(status -> status.equals("ACTIVE"))
          .waitForResult();
  }

  private boolean exists(String stream) {
    try {
      firehoseClient.describeDeliveryStream(new DescribeDeliveryStreamRequest()
        .withDeliveryStreamName(stream));
      return true;
    } catch (ResourceNotFoundException e) {
      return false;
    }
  }


  public List<ByteBuffer> read(String streamName) {
    String prefix = streams.getS3Path(streamName);
    long timeout = streams.getTimeout(prefix);

    // read the data from S3 to ensure it matches the raw data sent
    ResultWaiter<ObjectListing> wait =
      waiter.get()
            .withInterval(READ_S3_INTERVAL)
            .withTimeout(Math.max(timeout, READ_S3_INTERVAL))
            .withDescription(
              "Firehose -> s3 [" + TestProperties.Firehose.S3_BUCKET_NAME + "/" + prefix + "] "
              + "flush; max expceted: ~60sec")
            .withStatus(() -> s3.listObjects(TestProperties.Firehose.S3_BUCKET_NAME, prefix))
            .withStatusCheck(listing -> ((ObjectListing) listing).getObjectSummaries().size() > 0);
    if (!wait.waitForResult()) {
      return new ArrayList<>(0);
    }

    // read the most recent record from the bucket with our prefix
    ObjectListing listing = wait.getLastStatus();
    Optional<S3ObjectSummary> optionalSummary =
      listing.getObjectSummaries().stream()
             .peek(summary -> LOG.debug("Got s3 location: " + summary.getKey()))
             .sorted((s1, s2) -> {
               ZonedDateTime time = parseTimeFromS3ObjectName(s1.getKey(), streamName);
               ZonedDateTime t2 = parseTimeFromS3ObjectName(s2.getKey(), streamName);
               // descending order
               return -time.compareTo(t2);
             }).findFirst();
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

  private ZonedDateTime parseTimeFromS3ObjectName(String name, String stream) {
    name = name.substring(name.lastIndexOf('/') + 1);
    name = name.replaceFirst(stream, "");
    name = name.replaceFirst("-1-", "");
    int[] i = Stream.of(name.split("-")).limit(6).mapToInt(Integer::valueOf).toArray();
    return ZonedDateTime.of(LocalDateTime.of(i[0], i[1], i[2], i[3], i[4], i[5]), ZoneId.of("Z"));
  }

  @Override
  public void cleanup(FutureWaiter futures) {
    streams.firehoseNames().forEach(streamName -> futures.run(() -> {
      DeleteDeliveryStreamRequest delete = new DeleteDeliveryStreamRequest()
        .withDeliveryStreamName(streamName);
      ResultWaiter.doOrNull(() -> firehoseClient.deleteDeliveryStream(delete));
      waiter.get()
            .withDescription("Ensuring Firehose delete of: " + streamName)
            .withStatusNull(
              () -> firehoseClient.describeDeliveryStream(new DescribeDeliveryStreamRequest()
                .withDeliveryStreamName(streamName)))
            .waitForResult();
    }));

    futures.run(this::cleanupData);
  }

  public void cleanupData() {
    // remove all s3 files with the current test prefix
    S3Resource delete = new S3Resource(provider).withBucket(TestProperties.Firehose.S3_BUCKET_NAME);
    delete.delete(props.getTestPrefix());
  }

  public void ensureNoDataStored() {
    List<S3ObjectSummary> objs =
      s3.listObjects(TestProperties.Firehose.S3_BUCKET_NAME, props.getTestPrefix())
        .getObjectSummaries();
    assertEquals("There were some s3 files created! Files: " + Joiner.on(',').join(
      objs.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList())), 0, objs.size());
  }

  public void clone(List<Pair<String, LambdaClientProperties.StreamType>> toClone, File dir)
    throws IOException {
    for (Pair<String, LambdaClientProperties.StreamType> stream : toClone) {
      String name = props.getFirehoseStreamName(stream.getKey(), stream.getValue());
      File file = new File(dir, name);
      if (file.exists()) {
        LOG.info("Skipping copying data for file: " + file);
        continue;
      }
      ResourceUtils.writeStream(name, dir, () -> this.read(name));
    }
  }
}
