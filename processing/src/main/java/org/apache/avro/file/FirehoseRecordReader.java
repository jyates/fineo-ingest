package org.apache.avro.file;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reader of a firehose written file. Records are assumed to have been written
 */
public class FirehoseRecordReader<D> {

  public static final int OFFSET_COUNT_LENGTH = 4;
  private final GenericDatumReader<D> datum;
  private final TranslatedSeekableInput translated;
  private DataFileReader<D> reader;

  private FirehoseRecordReader(TranslatedSeekableInput input, GenericDatumReader<D> reader) throws IOException {
    this.datum = reader;
    this.translated = input;
  }

  public D next() throws IOException {
    return next(null);
  }

  public D next(D reuse) throws IOException {
    // create a new reader
    if ((reader == null || !reader.hasNext())) {
      if (!moreData()) {
        return null;
      }
      datum.setExpected(null);
      // seek out the next length into a byte array
      this.translated.nextBlock(OFFSET_COUNT_LENGTH);
      byte[] bytes = new byte[OFFSET_COUNT_LENGTH];
      translated.read(bytes, 0, bytes.length);
      // bytebuffer is better than DataInputStream here b/c DIS chokes on some lengths when reading
      // back 4 bytes...yeah, I dunno.
      int recordLength = readInt(bytes);
      translated.nextBlock(recordLength);
      reader = new DataFileReader<>(translated, datum);
      return next(reuse);
    }
    return reader.next(reuse);

  }

  private boolean moreData() throws IOException {
    return translated.remainingTotal() > 0;
  }

  /**
   * BigEndian byte read, as written by a bytebuffer, but without having to instantiate a
   * bytebuffer.
   *
   * @param bytes
   * @return
   */
  private int readInt(byte[] bytes) {
    return (
      ((bytes[0]) << 24) |
      ((bytes[1] & 0xff) << 16) |
      ((bytes[2] & 0xff) << 8) |
      ((bytes[3] & 0xff)));
  }

  public static FirehoseRecordReader<GenericRecord> create(ByteBuffer data) throws IOException {
    return new FirehoseRecordReader<>(new TranslatedSeekableInput(0, 0,
      SeekableByteBufferInput.create(data)), new GenericDatumReader<>());
  }
}