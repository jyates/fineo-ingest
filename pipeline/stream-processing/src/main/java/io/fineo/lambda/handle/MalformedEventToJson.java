package io.fineo.lambda.handle;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import io.fineo.internal.customer.Malformed;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Write {@link Malformed} events to JSON
 */
public class MalformedEventToJson implements AvroEventHandler<Malformed> {

  private static final Logger LOG = LoggerFactory.getLogger(MalformedEventToJson.class);

  private static final byte[] NEWLINE = "\n" .getBytes();
  private final GenericDatumWriter<GenericRecord> writer;
  private final ByteArrayOutputStream baos;
  private final JsonEncoder jsonEncoder;

  public MalformedEventToJson() {
    this.writer = new GenericDatumWriter<>(Malformed.SCHEMA$);
    this.baos = new ByteArrayOutputStream();
    try {
      this.jsonEncoder = EncoderFactory.get().jsonEncoder(Malformed.SCHEMA$, baos);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create malformed event converter", e);
    }
  }

  @Override
  public ByteBuffer write(Malformed record) throws IOException {
    writer.write(record, jsonEncoder);
    jsonEncoder.flush();
    baos.write(NEWLINE);
    baos.flush();
    LOG.error("JSON encoded malformed record: " + baos);
    // copy out the bytes for the byte buffer
    byte[] data = Arrays.copyOf(baos.toByteArray(), baos.size());
    baos.reset();
    return ByteBuffer.wrap(data);
  }

  public static Module getModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AvroEventHandler.class).toInstance(new MalformedEventToJson());
      }
    };
  }
}
