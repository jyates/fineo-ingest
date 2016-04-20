package io.fineo.batch.processing.spark.options;

import java.util.Properties;

/**
 *Bean class to handle the actual options for the batch processing
 */
public class BatchOptions {

  private String ingestPath;
  private Properties props;

  public String ingest() {
    return this.ingestPath;
  }

  public Properties props() {
    return props;
  }

  public void setInput(String fileAbsolutePath) {
    this.ingestPath = fileAbsolutePath;
  }

  public void setProps(Properties props) {
    this.props = props;
  }
}
