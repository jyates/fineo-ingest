package io.fineo.etl;

import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 *
 */
public class SeekableDataInput implements SeekableInput {
  private final FSDataInputStream in;
  private final int length;

  public SeekableDataInput(FSDataInputStream in) throws IOException {
    this.in = in;
    this.length = in.available();
  }

  @Override
  public void seek(long p) throws IOException {
    in.seek(p);
  }

  @Override
  public long tell() throws IOException {
    return in.getPos();
  }

  @Override
  public long length() throws IOException {
    return length;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
