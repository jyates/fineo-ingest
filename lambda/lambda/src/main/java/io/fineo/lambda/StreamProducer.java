package io.fineo.lambda;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.producer.KinesisProducer;

/**
 *
 */
public interface StreamProducer {

  void setDownstreamForTesting(io.fineo.lambda.avro.KinesisProducer producer);
}
