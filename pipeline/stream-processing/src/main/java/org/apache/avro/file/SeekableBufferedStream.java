package org.apache.avro.file;

import org.apache.avro.file.SeekableInput;

import java.io.BufferedInputStream;
import java.io.IOException;

/**
 * A seekable input stream that wraps a {@link BufferedInputStream}. Only supports data seeks up
 * to {@value Integer#MAX_VALUE}.
 */
public class SeekableBufferedStream implements SeekableInput {

  private final BufferedInputStream in;
  private final int length;

  public SeekableBufferedStream(BufferedInputStream in) throws IOException {
    this.in = in;
    this.length = in.available();
    in.mark(in.available());
  }

  @Override
  public void seek(long p) throws IOException {
    long position = tell();
    if (position < p) {
      in.skip(p - position);
    } else {
      in.reset();
      in.skip(p);
    }
  }

  @Override
  public long tell() throws IOException {
    return length - in.available();
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
