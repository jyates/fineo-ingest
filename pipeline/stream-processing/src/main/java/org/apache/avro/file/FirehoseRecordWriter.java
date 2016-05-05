package org.apache.avro.file;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Writes an Avro {@link org.apache.avro.generic.GenericRecord} to a format that we can read back
 * from the firehose via the {@link FirehoseRecordReader}.
 * <p>
 * Actual file layout is as follows:
 * <pre>
 *   4 bytes - integer length of record formatted by DataFileWriter
 *  ---> DataFile format
 *   4 bytes - DataFile magic
 *   [map of parts]
 *   [content]
 *  ---> END DataFile format
 * </pre>
 * </p>
 */
public class FirehoseRecordWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FirehoseRecordWriter.class);
  private static final byte VERSION = 1;
  public static final byte[] MAGIC = new byte[]{
    (byte) '1', (byte) 'j', (byte) 'k', VERSION
  };
  private static final byte[] recordLengthSpacer = new byte[]{0, 0, 0, 0};
  private CodecFactory codec;
  private DeflatorFactory deflator;

  public FirehoseRecordWriter() {
  }

  public FirehoseRecordWriter setCodec(CodecFactory factory) {
    this.codec = factory;
    return this;
  }

  public FirehoseRecordWriter setDeflator(DeflatorFactory factory) {
    this.deflator = factory;
    return this;
  }

  public ByteBuffer write(GenericRecord record) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(MAGIC);
    // Leave an 2 ints of room at the front so we can keep track metadata
    baos.write(recordLengthSpacer);
    baos.write(recordLengthSpacer);
    int offset = MAGIC.length + (2 * recordLengthSpacer.length);
    OutputStream out = deflator == null? baos : new BufferedOutputStream(deflator.deflate(baos));

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
    int len = bb.limit() - offset;
    bb.putInt(MAGIC.length, DeflatorFactory.DeflatorFactoryEnum.ordinalOf(deflator));
    bb.putInt(MAGIC.length + recordLengthSpacer.length, len);
    return bb;
  }

  public static FirehoseRecordWriter create() {
    return new FirehoseRecordWriter()
      .setCodec(CodecFactory.snappyCodec())
      .setDeflator(DeflatorFactory.gzip());
  }
}
