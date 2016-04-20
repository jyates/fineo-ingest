package io.fineo.batch.processing.spark.options;

import java.util.Properties;

/**
 *Bean class to handle the actual options for the batch processing
 */
public class BatchOptions {

  private String ingestPath;
  private String processingErrors;
  private String commitErrors;
  private String rawToAvroArchivePath;
  private String outputPath;

  public String ingest() {
    return this.ingestPath;
  }

  public Properties props() {
    return null;
  }
}
