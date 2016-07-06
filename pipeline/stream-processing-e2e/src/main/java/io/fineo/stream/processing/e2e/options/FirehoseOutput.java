package io.fineo.stream.processing.e2e.options;

import com.beust.jcommander.Parameter;

public class FirehoseOutput {

  @Parameter(names = "--firehose-output",
             description = "File to which we should write the firehose output from the storage "
                           + "stage (the input for batch processing)")
  public String outputFile;

  public String get() {
    return outputFile;
  }
}
