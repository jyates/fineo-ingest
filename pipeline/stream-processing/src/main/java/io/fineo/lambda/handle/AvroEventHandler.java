package io.fineo.lambda.handle;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public interface AvroEventHandler<T extends GenericRecord> {

  ByteBuffer write(T record) throws IOException;
}
