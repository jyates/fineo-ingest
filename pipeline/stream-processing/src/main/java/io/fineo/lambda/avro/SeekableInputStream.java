/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fineo.lambda.avro;

import org.apache.avro.file.SeekableInput;

import java.io.IOException;
import java.io.InputStream;

/**
 * Copied from org.apache.avro.file.DataFileReader$SeekableInputStream in Avro 1.7.7
 */
public class SeekableInputStream extends InputStream
  implements SeekableInput {
  private final byte[] oneByte = new byte[1];
  private SeekableInput in;

  SeekableInputStream(SeekableInput in) throws IOException {
    this.in = in;
  }

  @Override
  public void seek(long p) throws IOException {
    if (p < 0)
      throw new IOException("Illegal seek: " + p);
    in.seek(p);
  }

  @Override
  public long tell() throws IOException {
    return in.tell();
  }

  @Override
  public long length() throws IOException {
    return in.length();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return in.read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  @Override
  public int read() throws IOException {
    int n = read(oneByte, 0, 1);
    if (n == 1) {
      return oneByte[0] & 0xff;
    } else {
      return n;
    }
  }

  @Override
  public long skip(long skip) throws IOException {
    long position = in.tell();
    long length = in.length();
    long remaining = length - position;
    if (remaining > skip) {
      in.seek(skip);
      return in.tell() - position;
    } else {
      in.seek(remaining);
      return in.tell() - position;
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
    super.close();
  }

  @Override
  public int available() throws IOException {
    long remaining = (in.length() - in.tell());
    return (remaining > Integer.MAX_VALUE) ? Integer.MAX_VALUE
                                           : (int) remaining;
  }
}
