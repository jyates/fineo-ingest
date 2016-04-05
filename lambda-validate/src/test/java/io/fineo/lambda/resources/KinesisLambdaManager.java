package io.fineo.lambda.resources;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.CreateEventSourceMappingRequest;
import com.amazonaws.services.lambda.model.CreateEventSourceMappingResult;
import com.amazonaws.services.lambda.model.DeleteEventSourceMappingRequest;
import com.amazonaws.services.lambda.model.EventSourceMappingConfiguration;
import com.amazonaws.services.lambda.model.EventSourcePosition;
import com.amazonaws.services.lambda.model.ListEventSourceMappingsResult;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.kinesis.KinesisStreamManager;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manage interactions with Kinesis and the lamdba functions
 */
public class KinesisLambdaManager {
  private static final Log LOG = LogFactory.getLog(KinesisLambdaManager.class);
  private final LambdaClientProperties props;
  private final AWSCredentialsProvider credentials;
  private final AWSLambdaClient lambda;
  private final ResultWaiter.ResultWaiterFactory waiter;
  private final KinesisStreamManager kinesis;
  private Map<String, String> kinesisStreams = new HashMap<>(1);
  private List<String> kinesisToLambdaUUIDs = new ArrayList<>(1);

  public KinesisLambdaManager(LambdaClientProperties props, AWSCredentialsProvider provider,
    ResultWaiter.ResultWaiterFactory waiter) {
    this.props = props;
    this.credentials = provider;
    this.waiter = waiter;
    this.lambda = new AWSLambdaClient(credentials);
    this.kinesis = new KinesisStreamManager(this.credentials, waiter);
  }

  public void setup(String region) {
    // setup the kinesis stream
    String streamName = props.getRawToStagedKinesisStreamName();
    String arn = String.format(TestProperties.Kinesis.KINESIS_STREAM_ARN_TO_FORMAT, region,
      props.getRawToStagedKinesisStreamName());
    kinesis.setup(region, arn, streamName, TestProperties.Kinesis.SHARD_COUNT);

    // link that stream as an event source to the lambda function
    linkStreamToLambda(arn, region);
    LOG.debug("Kinesis setup complete!");
  }

  private void linkStreamToLambda(String arn, String region) {
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

  public List<ByteBuffer> getWrites(String stream) {
    return kinesis.getWrites(stream);
  }

  public void deleteStreams(FutureWaiter futures) {
    this.kinesis.deleteStreams(futures);

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
