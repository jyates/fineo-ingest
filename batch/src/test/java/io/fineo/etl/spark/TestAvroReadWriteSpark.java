package io.fineo.etl.spark;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import io.fineo.etl.options.ETLOptions;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.e2e.TestEndToEndLambdaLocal;
import io.fineo.lambda.e2e.TestOutput;
import io.fineo.lambda.util.ResourceManager;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.StreamType.ARCHIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Simple test class that ensures we can read/write avro files from spark
 */
public class TestAvroReadWriteSpark extends SharedJavaSparkContext {

  private static final Log LOG = LogFactory.getLog(TestAvroReadWriteSpark.class);
  @Rule
  public TestOutput folder = new TestOutput(false);

  @Before
  public void runBefore() {
    conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "io.fineo.etl.AvroKyroRegistrator");
    super.runBefore();
  }

  @Test
  public void testSingleRecord() throws Exception {
    TestEndToEndLambdaLocal.TestState state = TestEndToEndLambdaLocal.runTest();
    ResourceManager resources = state.getResources();

    // save the record(s) to a file
    File ingest = folder.newFolder("ingest");
    File file = new File(ingest, "output");
    File file2 = new File(ingest, "output2");

    LambdaClientProperties props = state.getRunner().getProps();
    String stream = props.getFirehoseStreamName(STAGED_PREFIX, ARCHIVE);
    writeStreamToFile(resources, stream, file);
    writeStreamToFile(resources, stream, file2);

    // open the file in spark and get the instances
    File archive = folder.newFolder("archive");
    File archiveOut = new File(archive, "output");
    ETLOptions opts = new ETLOptions();
    String base = "file://";
    opts.archive(base + archiveOut.getAbsolutePath());
    opts.root(base + ingest.getAbsolutePath());

    SparkETL etl = new SparkETL(opts);
    etl.run(jsc());

    ensureOnlyOneRecordWithData(archiveOut, "part-00000.gz", "part-00001.gz");

  }

  private void ensureOnlyOneRecordWithData(File archive, String... paths)
    throws IOException {
    Configuration conf = new Configuration();
    GzipCodec codec = new GzipCodec();
    codec.setConf(conf);

    List<Pair<File, Integer>> files = new ArrayList<>(paths.length);
    for (String path : paths) {
      File file = new File(archive, path);
      FileInputStream stream = new FileInputStream(file);
      files.add(new ImmutablePair<>(file, codec.createInputStream(stream).read()));
    }
    assertEquals("Expected only one output file with data, but got " + files, 1,
      files.stream().map(pair -> pair.getRight()).filter(bytes -> bytes > 0).count());
  }

  private void writeStreamToFile(ResourceManager resources, String stream, File file)
    throws IOException {
    FileOutputStream fout = new FileOutputStream(file);
    FileChannel channel = fout.getChannel();
    resources.getFirehoseWrites(stream).stream().forEach(buff -> {
      buff.flip();
      try {
        channel.write(buff);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    channel.close();

  }
}
