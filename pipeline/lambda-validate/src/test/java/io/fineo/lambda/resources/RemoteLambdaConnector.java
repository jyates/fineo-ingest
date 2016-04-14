package io.fineo.lambda.resources;

import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.CreateEventSourceMappingRequest;
import com.amazonaws.services.lambda.model.CreateEventSourceMappingResult;
import com.amazonaws.services.lambda.model.DeleteEventSourceMappingRequest;
import com.amazonaws.services.lambda.model.EventSourcePosition;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.util.run.FutureWaiter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Connect the named kinesis stream names to remote lambda functions, described by their ARN
 */
public class RemoteLambdaConnector extends LambdaKinesisConnector<String> {

  private static final Log LOG = LogFactory.getLog(RemoteLambdaConnector.class);

  private final String region;
  private IKinesisStreams kinesis;
  private List<String> kinesisToLambdaUUIDs = new ArrayList<>(1);
  private AWSLambdaClient lambda;

  public RemoteLambdaConnector(
    Map<String, List<String>> streamToLambdaMapping, String pipelineSource, String region) {
    super(streamToLambdaMapping, pipelineSource);
    this.region = region;
  }

  @Override
  public void write(String kinesisStream, byte[] data) {
    this.kinesis.submit(kinesisStream, ByteBuffer.wrap(data));
  }

  @Override
  public void connect(LambdaClientProperties props, IKinesisStreams kinesis) throws IOException {
    this.kinesis = kinesis;

    this.lambda = new AWSLambdaClient(props.getProvider());

    // create each stream
    for (String stream : this.mapping.keySet()) {
      LOG.info("Creating kinesis stream: " + stream);
      kinesis.setup(stream);
    }

    // connect the streams to the functions
    for (Map.Entry<String, List<String>> streamToArn : mapping.entrySet()) {
      String arn = String
        .format(TestProperties.Kinesis.KINESIS_STREAM_ARN_TO_FORMAT, region, streamToArn.getKey());
      for (String functionArn : streamToArn.getValue()) {
        LOG.info("Mapping Kinesis: '" + arn + "' => lambda: '" + functionArn + "'");
        CreateEventSourceMappingRequest mapping = new CreateEventSourceMappingRequest();
        mapping.setFunctionName(functionArn);
        mapping.setEventSourceArn(arn);
        mapping.setBatchSize(1);
        mapping.setStartingPosition(EventSourcePosition.TRIM_HORIZON);
        mapping.setEnabled(true);
        CreateEventSourceMappingResult result = lambda.createEventSourceMapping(mapping);
        this.kinesisToLambdaUUIDs.add(result.getUUID());
        LOG.info("Created event mapping: " + result);
      }
    }
  }

  @Override
  public List<ByteBuffer> getWrites(String streamName) {
    List<ByteBuffer> data = new ArrayList<>();
    for (List<ByteBuffer> buffs : this.kinesis.getEventQueue(streamName)) {
      data.addAll(buffs);
    }
    return data;
  }

  @Override
  public void cleanup(FutureWaiter futures) {
    // delete the connections between lambda and kinesis
    for (String uuid : kinesisToLambdaUUIDs) {
      futures.run(() ->
        lambda.deleteEventSourceMapping(new DeleteEventSourceMappingRequest().withUUID(uuid)));
    }
  }
}
