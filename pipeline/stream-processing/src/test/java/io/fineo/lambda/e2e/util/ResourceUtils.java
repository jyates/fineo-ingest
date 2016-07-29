package io.fineo.lambda.e2e.util;

import io.fineo.lambda.e2e.manager.collector.OutputCollector;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.function.Supplier;

/**
 *
 */
public class ResourceUtils {

  public static void writeStream(String fileName, OutputCollector directory,
    Supplier<Collection<ByteBuffer>> dataSupplier) throws IOException {
    OutputStream stream = directory.get(fileName);
    Collection<ByteBuffer> writes = dataSupplier.get();
    WritableByteChannel out = Channels.newChannel(new BufferedOutputStream(stream));
    for (ByteBuffer data : writes) {
      out.write(data);
    }
    out.close();
  }
}
