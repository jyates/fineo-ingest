package io.fineo.batch.processing.spark;

import io.fineo.lambda.e2e.resources.aws.lambda.LambdaKinesisConnector;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Simple Kinesis connector that doesn't pass messages between stages, but does write down the
 * messages for consumption
 */
public class SparkLambdaKinesisConnector extends LambdaKinesisConnector<Object> {

  private static final Log LOG = LogFactory.getLog(SparkLambdaKinesisConnector.class);

  private final JavaSparkContext context;
  private final Map<String, File> folders = new HashMap<>();
  private final TestOutput output;
  private final LocalMockBatchOptions opts;

  public SparkLambdaKinesisConnector(TestOutput directory, LocalMockBatchOptions opts,
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
      Instant start = Instant.now();
      new BatchProcessor(opts).run(context);
      Instant end = Instant.now();
      Duration duration = Duration.between(start, end);
      LOG.info("Batch job took: "+ duration.toMillis()+" ms OR"+duration.toMinutes()+" mins");
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
