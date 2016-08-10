package io.fineo.stream.processing.e2e.options;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonArgument {

  @Parameter(names = "--json", description = "Path to the json files with the event to send")
  public String json;

  public List<Map<String, Object>> get() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new File(json), new TypeReference<List<Map<String, Object>>>(){});
  }
}
