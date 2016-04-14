package io.fineo.lambda;

/**
 *
 */
public interface StreamProducer {

  void setDownstreamForTesting(io.fineo.lambda.kinesis.KinesisProducer producer);
}
