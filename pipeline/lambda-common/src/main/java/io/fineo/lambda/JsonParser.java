package io.fineo.lambda;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.AbstractIterator;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * Simple wrapper around parsing facilities to support streams and strings
 */
public class JsonParser implements Serializable{

  private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);
  private final JsonFactory jsonFactory;

  public JsonParser() {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.setCodec(new ObjectMapper());
    this.jsonFactory = jsonFactory;
  }

  public Iterable<Map<String, Object>> parse(String json) throws IOException {
    return parse(new ByteArrayInputStream(json.getBytes()));
  }

  public Iterable<Map<String, Object>> parse(InputStream stream) throws IOException {
    com.fasterxml.jackson.core.JsonParser jp = jsonFactory.createParser(logStream(stream));
    return () -> new AbstractIterator<Map<String, Object>>() {
      com.fasterxml.jackson.core.JsonToken token;

      @Override
      protected Map<String, Object> computeNext() {
        try {
          while ((token = jp.nextToken()) != null) {
            switch (token) {
              case START_OBJECT:
                return jp.readValueAs(Map.class);
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        endOfData();
        return null;
      }
    };
  }

  private InputStream logStream(InputStream stream) throws IOException {
    if (!LOG.isTraceEnabled()) {
      return stream;
    }

    String json = IOUtils.toString(stream, "UTF-8");
    LOG.trace("Got message: " + json);
    return new ByteArrayInputStream(json.getBytes());
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    jsonFactory.setCodec(new ObjectMapper());
  }
}
