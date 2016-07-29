package io.fineo.lambda.e2e.resources.manager.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class LoggingCollector implements OutputCollector {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingCollector.class);
  private final String prefix;

  public LoggingCollector() {
    this("");
  }

  public LoggingCollector(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public OutputCollector getNextLayer(String name) {
    return new LoggingCollector(this.prefix.length() == 0 ? name : prefix + "." + name);
  }

  @Override
  public OutputStream get(String name) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    return new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        bos.write(b);
      }

      @Override
      public void close() throws IOException {
        String out = new String(bos.toByteArray());
        LOG.info((prefix.length() == 0 ? name : prefix + "." + name) + ": " + out);
      }
    };
  }

  @Override
  public String getRoot() {
    return "LOGS - See Above";
  }
}
