package io.fineo.etl.spark;

/**
 *Bean class to handle the actual options for the batch processing
 */
public class BatchOptions {

  private String ingestPath;
  private String processingErrors;
  private String commitErrors;
  private String rawToAvroArchivePath;
  private String outputPath;
}
