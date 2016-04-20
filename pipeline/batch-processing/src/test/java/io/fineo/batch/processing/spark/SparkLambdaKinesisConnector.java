package io.fineo.batch.processing.spark;

import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.lambda.e2e.resources.aws.lambda.LambdaKinesisConnector;
import io.fineo.test.rule.TestOutput;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Simple Kinesis connector that doesn't pass messages between stages, but does write down the
 * messages for consumption
 */
public class SparkLambdaKinesisConnector extends LambdaKinesisConnector<Object> {

  private final TestOutput output;
  private final BatchOptions opts;

  public SparkLambdaKinesisConnector(TestOutput directory, BatchOptions opts) {
    this.output = directory;
    this.opts = opts;
  }

  @Override
  public void write(String kinesisStream, byte[] data) throws IOException {
    // write the bytes to a file
    File file = output.newFile();
    try (FileOutputStream out = new FileOutputStream(file)) {
      out.write(data);
    }
    opts.setInput(file.getAbsolutePath());
  }
}
