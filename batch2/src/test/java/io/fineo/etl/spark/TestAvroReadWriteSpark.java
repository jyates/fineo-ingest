package io.fineo.etl.spark;

import io.fineo.lambda.e2e.TestEndToEndLambdaLocal;
import io.fineo.lambda.e2e.TestOutput;
import io.fineo.lambda.util.ResourceManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Simple test class that ensures we can read/write avro files from spark
 */
public class TestAvroReadWriteSpark {

  private static final Log LOG = LogFactory.getLog(TestAvroReadWriteSpark.class);
  @Rule
  public TestOutput folder = new TestOutput(false);

  @Test
  public void test() throws Exception {
    TestEndToEndLambdaLocal.TestState state = TestEndToEndLambdaLocal.runTest();
    ResourceManager resources = state.getResources();

    // save the record(s) to a file
    File file = folder.newFile();

    FileOutputStream fout = new FileOutputStream(file);
    FileChannel channel = fout.getChannel();
    channel.write(util.getFirehoseWrites(archived).toArray(new ByteBuffer[0]));
    channel.close();

    // open the file in spark and get the instances

  }
}
