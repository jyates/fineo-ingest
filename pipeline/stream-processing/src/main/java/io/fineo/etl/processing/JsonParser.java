package io.fineo.etl.processing;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Simple wrapper around parsing facilities to support streams and strings
 */
public class JsonParser {

  private static final Log LOG = LogFactory.getLog(JsonParser.class);
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
    com.fasterxml.jackson.core.JsonToken token;
    List<Map<String, Object>> messages = new ArrayList<>();
    while ((token = jp.nextToken()) != null) {
      switch (token) {
        case START_OBJECT:
          messages.add(jp.readValueAs(Map.class));
          break;
      }
    }
    return messages;
  }

  private InputStream logStream(InputStream stream) throws IOException {
    if (!LOG.isTraceEnabled()) {
      return stream;
    }

    String json = IOUtils.toString(stream, "UTF-8");
    LOG.trace("Got message: " + json);
    return new ByteArrayInputStream(json.getBytes());
  }
}
