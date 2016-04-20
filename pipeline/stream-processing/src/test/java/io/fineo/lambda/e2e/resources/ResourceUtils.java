package io.fineo.lambda.e2e.resources;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.function.Supplier;

/**
 *
 */
public class ResourceUtils {

  public static void writeStream(String fileName, File directory,
    Supplier<List<ByteBuffer>> dataSupplier) throws IOException {
    File file = new File(directory, fileName);
    List<ByteBuffer> writes = dataSupplier.get();
    WritableByteChannel out = Channels.newChannel(new BufferedOutputStream(new FileOutputStream
      (file)));
    for (ByteBuffer data : writes) {
      out.write(data);
    }
    out.close();
  }
}
