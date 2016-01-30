package io.fineo.lambda.resources;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.CreateEventSourceMappingRequest;
import com.amazonaws.services.lambda.model.CreateEventSourceMappingResult;
import com.amazonaws.services.lambda.model.DeleteEventSourceMappingRequest;
import com.amazonaws.services.lambda.model.EventSourceMappingConfiguration;
import com.amazonaws.services.lambda.model.EventSourcePosition;
import com.amazonaws.services.lambda.model.ListEventSourceMappingsResult;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.TestProperties;
import io.fineo.lambda.util.FutureWaiter;
import io.fineo.lambda.util.ResultWaiter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;

/**
 * Manage interactions with Kinesis streams
 */
public class KinesisManager {
  private static final Log LOG = LogFactory.getLog(KinesisManager.class);
  private final LambdaClientProperties props;
  private final AWSCredentialsProvider credentials;
  private final AWSLambdaClient lambda;
  private Map<String, String> kinesisStreams = new HashMap<>(1);
  private String region;
  private AmazonKinesisClient kinesis;
  private List<String> kinesisToLambdaUUIDs = new ArrayList<>(1);

  public KinesisManager(LambdaClientProperties props, AWSCredentialsProvider provider) {
    this.props = props;
    this.credentials = provider;
    this.lambda = new AWSLambdaClient(credentials);
  }

  public void setup(String region) {
    this.region = region;
    this.kinesis = getKinesis();
    String streamName = props.getRawToStagedKinesisStreamName();
    String arn = String.format(TestProperties.Kinesis.KINESIS_STREAM_ARN_TO_FORMAT, region,
      props.getRawToStagedKinesisStreamName());
    kinesisStreams.put(streamName, arn);

    assertTrue("Kinesis did not get setup within time limit", setupKinesisStreams(streamName));

    linkStreamToLambda(arn);
    LOG.debug("Kinesis setup complete!");
  }

  private void linkStreamToLambda(String arn) {
    String func = TestProperties.Lambda.getAvroToStoreArn(region);
    LOG.info("Creating kinesis -> lambda link");
    ListEventSourceMappingsResult sourceMappings = lambda.listEventSourceMappings();
    for (EventSourceMappingConfiguration map : sourceMappings.getEventSourceMappings()) {
      if (map.getEventSourceArn().endsWith(arn)) {
        LOG.info("Mapping from " + map.getEventSourceArn() + " -> " + func + " already exists!");
        this.kinesisToLambdaUUIDs.add(map.getUUID());
        return;
      }
    }

    CreateEventSourceMappingRequest mapping = new CreateEventSourceMappingRequest();
    mapping.setFunctionName(func);
    mapping.setEventSourceArn(arn);
    mapping.setBatchSize(1);
    mapping.setStartingPosition(EventSourcePosition.LATEST);
    mapping.setEnabled(true);
    CreateEventSourceMappingResult result = lambda.createEventSourceMapping(mapping);
    this.kinesisToLambdaUUIDs.add(result.getUUID());
    LOG.info("Created event mapping: " + result);
  }

  private boolean setupKinesisStreams(String stream) {
    CreateStreamRequest createStreamRequest = new CreateStreamRequest();
    createStreamRequest.setStreamName(stream);
    createStreamRequest.setShardCount(TestProperties.Kinesis.SHARD_COUNT);
    kinesis.createStream(createStreamRequest);
    return new ResultWaiter<>()
      .withDescription("Kinesis stream:" + stream + " activation")
      .withStatus(() -> {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(stream);
        DescribeStreamResult describeStreamResponse =
          kinesis.describeStream(describeStreamRequest);
        return describeStreamResponse.getStreamDescription().getStreamStatus();
      }).withStatusCheck(streamStatus -> streamStatus.equals("ACTIVE"))
      .waitForResult();
  }

  private AmazonKinesisClient getKinesis() {
    AmazonKinesisClient client = new AmazonKinesisClient(credentials);
    client.setRegion(RegionUtils.getRegion(region));
    return client;
  }

  public List<ByteBuffer> getWrites(String stream) {
    GetShardIteratorResult shard =
      kinesis.getShardIterator(stream, "0", "TRIM_HORIZON");
    String iterator = shard.getShardIterator();
    GetRecordsResult getResult = kinesis.getRecords(new GetRecordsRequest().withShardIterator
      (iterator));
    return getResult.getRecords().stream().map(Record::getData).collect(Collectors.toList());
  }

  public void deleteStreams(FutureWaiter futures) {
    // delete the streams
    kinesisStreams.keySet().stream().forEach(name -> futures.run(() -> {
      DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
      deleteStreamRequest.setStreamName(name);
      kinesis.deleteStream(deleteStreamRequest);

      new ResultWaiter<>()
        .withDescription("Ensure delete of kinesis stream: " + name)
        .withStatusNull(() -> kinesis.describeStream(name))
        .waitForResult();
    }));

    // delete the connections between lambda and kinesis
    for (String uuid : kinesisToLambdaUUIDs) {
      futures.run(() ->
        lambda.deleteEventSourceMapping(new DeleteEventSourceMappingRequest().withUUID(uuid)));
    }
  }

  public void clone(String streamName, File dir) throws IOException {
    ResourceUtils.writeStream(streamName, dir, () -> this.getWrites(streamName));
  }
}
