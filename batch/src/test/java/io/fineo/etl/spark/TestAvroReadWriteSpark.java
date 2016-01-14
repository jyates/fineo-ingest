package io.fineo.etl.spark;

import io.fineo.lambda.EndToEndTestUtil;
import io.fineo.lambda.LambdaTestUtils;
import io.fineo.lambda.avro.LambdaClientProperties;
import io.fineo.util.TemporaryFolderWithCleanupToggle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Properties;

/**
 * Simple test class that ensures we can read/write avro files from spark
 */
public class TestAvroReadWriteSpark {

  private static final Log LOG = LogFactory.getLog(TestAvroReadWriteSpark.class);
  @Rule
  public TemporaryFolder folder = new TemporaryFolderWithCleanupToggle();

  @Test
  public void test() throws Exception {
    Properties props = new Properties();
    String archived = "archived";
    props.setProperty(LambdaClientProperties.FIREHOSE_STAGED_STREAM_NAME, archived);
    props.setProperty(LambdaClientProperties.PARSED_STREAM_NAME, "parsed");

    EndToEndTestUtil util = new EndToEndTestUtil(props);
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    util.run(json);

    // save the record(s) to a file
    File file = folder.newFile();

    FileOutputStream fout = new FileOutputStream(file);
    FileChannel channel = fout.getChannel();
    channel.write(util.getFirehoseWrites(archived).toArray(new ByteBuffer[0]));
    channel.close();

    // open the file in spark and get the instances

  }
}