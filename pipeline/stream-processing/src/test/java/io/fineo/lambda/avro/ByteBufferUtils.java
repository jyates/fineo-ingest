package io.fineo.lambda.avro;

import java.nio.ByteBuffer;

/**
 *
 */
public class ByteBufferUtils {

  private ByteBufferUtils(){ }

  /**
   * Copy all but the first byte of the underlying buffer to create a new {@link ByteBuffer}
   * @param buff
   * @return
   */
  public static ByteBuffer skipFirstByteCopy(ByteBuffer buff){
    byte[] data = new byte[buff.array().length-1];
    System.arraycopy(buff.array(), 1, data, 0, buff.limit() -1);
    return ByteBuffer.wrap(data);
  }
}
