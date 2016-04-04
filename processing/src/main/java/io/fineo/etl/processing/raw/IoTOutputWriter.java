package io.fineo.etl.processing.raw;

import io.fineo.etl.processing.OutputWriter;
import io.fineo.lambda.aws.MultiWriteFailures;
import org.apache.avro.generic.GenericRecord;

/**
 * Writes avro events back to the IoT framework
 */
public class IoTOutputWriter implements OutputWriter<GenericRecord>{

  @Override
  public void write(GenericRecord obj) {

  }

  @Override
  public MultiWriteFailures commit() {
    return null;
  }
}
