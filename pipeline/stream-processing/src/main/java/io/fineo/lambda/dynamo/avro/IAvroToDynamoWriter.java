package io.fineo.lambda.dynamo.avro;

import io.fineo.internal.customer.BaseFields;
import io.fineo.lambda.aws.MultiWriteFailures;
import org.apache.avro.generic.GenericRecord;

/**
 *
 */
public interface IAvroToDynamoWriter {
  /**
   * Write the record to dynamo. Completes asynchronously, call {@link #flush()} to ensure all
   * records finish writing to dynamo. <b>non-blocking, thread-safe</b>.
   *
   * @param record record to write to dynamo. Expected to have at least a {@link BaseFields} field
   */
  void write(GenericRecord record);

  MultiWriteFailures<GenericRecord> flush();
}
