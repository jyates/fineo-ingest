package io.fineo.lambda.avro;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Writes an Avro {@link org.apache.avro.generic.GenericRecord} to a format that we can read back
 * from the firehose
 */
public class FirehoseWriter {

  private static final Log LOG = LogFactory.getLog(FirehoseWriter.class);
  private static final byte[] intSpace = new byte[]{0, 0, 0, 0};
  private CodecFactory codec;

  public FirehoseWriter() {
  }

  public FirehoseWriter setCodec(CodecFactory factory) {
    this.codec = factory;
    return this;
  }

  public ByteBuffer write(GenericRecord record) throws IOException {
    // Leave an int of room at the front so we can keep track of how big the record is and
    // read in just that amount
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream out = new BufferedOutputStream(baos);
    out.write(intSpace);

    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
    if (this.codec != null) {
      fileWriter.setCodec(this.codec);
    }
    fileWriter.create(record.getSchema(), out);
    fileWriter.append(record);
    fileWriter.close();
    out.flush();

    byte[] data = baos.toByteArray();
    // write out the actual length of the data
    ByteBuffer bb = ByteBuffer.wrap(data);
    int len = bb.limit() - 4;
    bb.putInt(0, len);

    return bb;
  }
}
