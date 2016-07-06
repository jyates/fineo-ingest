package io.fineo.lambda.handle.raw;

import io.fineo.internal.customer.Malformed;
import io.fineo.lambda.configure.firehose.FirehoseFunctions;
import io.fineo.lambda.avro.FirehoseRecordWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Module that creates a {@link Function} named <code>firehose.malformed.function</code> that maps
 * a set of raw bytes into a {@link Malformed} for writing to a firehose
 */
public class FirehoseToMalformedInstanceFunctionModule extends FirehoseFunctions {

  public FirehoseToMalformedInstanceFunctionModule() {
    super();
    setMalformed(func);
  }

  static final Function<ByteBuffer, ByteBuffer> func = data -> {
    // convert the data into a malformed record
    Malformed mal = Malformed.newBuilder().setRecordContent(data).build();
    // write it out into a new bytebuffer that we can read
    FirehoseRecordWriter writer = new FirehoseRecordWriter();
    try {
      return writer.write(mal);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  };
}
