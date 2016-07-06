package io.fineo.lambda.avro;

import com.google.common.base.Preconditions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteBufferInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.file.TranslatedSeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Reader of a {@link FirehoseRecordWriter} written file.
 */
public class FirehoseRecordReader<D> {

  public static final int OFFSET_COUNT_LENGTH = 4;
  private final GenericDatumReader<D> datum;
  private final TranslatedSeekableInput translated;
  private DataFileReader<D> reader;
  // reusing this bytes makes this very not thread-safe, but it is efficient!
  byte[] bytes = new byte[OFFSET_COUNT_LENGTH];

  private FirehoseRecordReader(TranslatedSeekableInput input, GenericDatumReader<D> reader)
    throws IOException {
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

      byte[] magic = readFourByteBlock();
      Preconditions.checkArgument(Arrays.equals(magic, FirehoseRecordWriter.MAGIC),
        "First byte does not match expected magic %s!",
        Arrays.toString(FirehoseRecordWriter.MAGIC));

      int deflator = readInt();
      int recordLength = readInt();
      translated.nextBlock(recordLength);
      SeekableInput currentInput;
      // if we have a deflator, wrap the current translator with the deflating translator
      if (DeflatorFactory.DeflatorFactoryEnum.NULL.ordinal() == deflator) {
        currentInput = translated;
      } else {
        DeflatorFactory factory =
          DeflatorFactory.DeflatorFactoryEnum.values()[deflator].getFactory();
        SeekableInputStream in = new SeekableInputStream(translated);
        InputStream stream = factory.inflate(in);
        // wrap that stream into a seekableinput
        currentInput = new SeekableBufferedStream(new BufferedInputStream(stream));
      }
      reader = new DataFileReader<>(currentInput, datum);
      return next(reuse);
    }
    return reader.next(reuse);
  }

  private int readInt() throws IOException {
    return readInt(readFourByteBlock());
  }

  private byte[] readFourByteBlock() throws IOException {
    // seek out the next length into a byte array
    this.translated.nextBlock(OFFSET_COUNT_LENGTH);
    translated.read(bytes, 0, bytes.length);
    return bytes;
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

  public static FirehoseRecordReader<GenericRecord> create(SeekableInput input) throws Exception {
    return new FirehoseRecordReader<>(new TranslatedSeekableInput(0, 0, input),
      new GenericDatumReader<>());
  }

  public static FirehoseRecordReader<GenericRecord> create(ByteBuffer data) throws IOException {
    return new FirehoseRecordReader<>(new TranslatedSeekableInput(0, 0,
      SeekableByteBufferInput.create(data)), new GenericDatumReader<>());
  }
}
