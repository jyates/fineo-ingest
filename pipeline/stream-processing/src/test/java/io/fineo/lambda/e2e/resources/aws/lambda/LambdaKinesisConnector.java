package io.fineo.lambda.e2e.resources.aws.lambda;

import io.fineo.lambda.e2e.resources.aws.AwsResource;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Connect Kinesis streams to Lambda functions
 */
public abstract class LambdaKinesisConnector<T> implements AwsResource {

  protected Map<String, List<T>> mapping;
  protected String source;
  protected IKinesisStreams kinesis;

  public void configure(Map<String, List<T>> streamToLambdaMapping, String pipelineSource) {
    this.mapping = streamToLambdaMapping;
    this.source = pipelineSource;
  }

  public void write(byte[] data) throws IOException {
    write(source, data);
  }

  public void write(String kinesisStream, byte[] data) throws IOException {
    this.kinesis.submit(kinesisStream, ByteBuffer.wrap(data));
  }

  public void connect(IKinesisStreams kinesis) throws IOException {
    this.kinesis = kinesis;
  }

  public List<ByteBuffer> getWrites(String streamName) {
    List<ByteBuffer> data = new ArrayList<>();
    for (List<ByteBuffer> buffs : this.kinesis.getEventQueue(streamName)) {
      data.addAll(buffs);
    }
    return data;
  }

  public void reset() {
  }
}
