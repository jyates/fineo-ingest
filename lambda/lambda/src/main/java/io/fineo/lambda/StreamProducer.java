package io.fineo.lambda;

import com.amazonaws.services.kinesis.producer.KinesisProducer;

/**
 *
 */
public interface StreamProducer {

  void setDownstreamForTesting(KinesisProducer producer);
}
