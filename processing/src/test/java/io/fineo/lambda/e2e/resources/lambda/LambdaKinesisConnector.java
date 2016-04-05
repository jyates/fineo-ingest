package io.fineo.lambda.e2e.resources.lambda;

import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.e2e.resources.AwsResource;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Connect Kinesis streams to Lambda functions
 */
public abstract class LambdaKinesisConnector<T> implements AwsResource {

  protected final Map<String, List<T>> mapping;
  protected final String source;

  public LambdaKinesisConnector(Map<String, List<T>> streamToLambdaMapping, String pipelineSource){
    this.mapping = streamToLambdaMapping;
    this.source = pipelineSource;
  }

  public abstract void write(byte[] data);

  public abstract void write(String kinesisStream, byte[] data);

  public abstract void connect(LambdaClientProperties props, IKinesisStreams kinesis) throws IOException;

  public abstract List<ByteBuffer> getWrites(String streamName);
}
