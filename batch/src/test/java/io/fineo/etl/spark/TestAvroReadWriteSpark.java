package io.fineo.etl.spark;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import io.fineo.etl.options.ETLOptions;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.e2e.TestEndToEndLambdaLocal;
import io.fineo.lambda.e2e.TestOutput;
import io.fineo.lambda.util.ResourceManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.StreamType.ARCHIVE;

/**
 * Simple test class that ensures we can read/write avro files from spark
 */
public class TestAvroReadWriteSpark extends SharedJavaSparkContext {

  private static final Log LOG = LogFactory.getLog(TestAvroReadWriteSpark.class);
  @Rule
  public TestOutput folder = new TestOutput(false);

  @Test
  public void test() throws Exception {
    TestEndToEndLambdaLocal.TestState state = TestEndToEndLambdaLocal.runTest();
    ResourceManager resources = state.getResources();

    // save the record(s) to a file
    File ingest = folder.newFolder("ingest");
    File file = new File(ingest, "output");


    FileOutputStream fout = new FileOutputStream(file);
    FileChannel channel = fout.getChannel();
    LambdaClientProperties props = state.getRunner().getProps();
    String stream = props.getFirehoseStreamName(STAGED_PREFIX, ARCHIVE);
    resources.getFirehoseWrites(stream).stream().forEach(buff -> {
      buff.flip();
      try {
        channel.write(buff);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    channel.close();

    // open the file in spark and get the instances
    File archive = folder.newFolder("archive");
    File archiveOut = new File(archive, "output");
    ETLOptions opts = new ETLOptions();
    String base = "file://";
    opts.archive(base + archiveOut.getAbsolutePath());
    opts.root(base + ingest.getAbsolutePath());

    SparkETL etl = new SparkETL(opts);
    etl.run(jsc());
  }
}
