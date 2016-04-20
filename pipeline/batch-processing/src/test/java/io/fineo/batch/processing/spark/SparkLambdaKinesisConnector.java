package io.fineo.batch.processing.spark;

import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.lambda.e2e.resources.aws.lambda.LambdaKinesisConnector;
import io.fineo.test.rule.TestOutput;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Simple Kinesis connector that doesn't pass messages between stages, but does write down the
 * messages for consumption
 */
public class SparkLambdaKinesisConnector extends LambdaKinesisConnector<Object> {

  private final JavaSparkContext context;
  private final Map<String, File> folders = new HashMap<>();
  private final TestOutput output;
  private final BatchOptions opts;

  public SparkLambdaKinesisConnector(TestOutput directory, BatchOptions opts,
    JavaSparkContext context) {
    this.output = directory;
    this.opts = opts;
    this.context = context;
  }

  @Override
  public void write(String kinesisStream, byte[] data) throws IOException {
    File dir = folders.get(kinesisStream);
    if (dir == null) {
      dir = output.newFolder(kinesisStream);
      folders.put(kinesisStream, dir);
    }
    // write the bytes to a file
    File file = new File(dir, UUID.randomUUID().toString());
    try (FileOutputStream out = new FileOutputStream(file)) {
      out.write(data);
    }
    opts.setInput(file.getAbsolutePath());

    // kick off the job
    try {
      new BatchProcessor(opts).run(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
