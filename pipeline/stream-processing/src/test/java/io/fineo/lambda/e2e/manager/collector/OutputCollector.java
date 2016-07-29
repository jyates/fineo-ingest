package io.fineo.lambda.e2e.manager.collector;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

public interface OutputCollector {

  OutputCollector getNextLayer(String name);
  OutputStream get(String name) throws FileNotFoundException, IOException;

  String getRoot();
}
