package io.fineo.batch.processing.spark;

import com.google.inject.Module;
import io.fineo.aws.AwsDependentTests;
import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.e2e.aws.BaseITEndToEndAwsServices;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.aws.firehose.FirehoseStreams;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.fineo.lambda.configure.legacy.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.configure.legacy.LambdaClientProperties.STAGED_PREFIX;
import static java.util.Arrays.asList;

@Category(AwsDependentTests.class)
public class ITBatchProcessingWithLocalSpark extends BaseITEndToEndAwsServices {

  @ClassRule
  public static LocalSparkRule spark = new LocalSparkRule();
  @Rule
  public TestOutput output = new TestOutput(false);
  private SparkFirehoseStreams firehoses;

  public ITBatchProcessingWithLocalSpark() {
    super(true);
  }

  @Test
  public void testHappyPath() throws Exception {
    String uuid = "integration-test-" + System.currentTimeMillis() + "-";
    Properties props = setProperties(uuid);

    // source dir is also the location for the 'archive'
    String source = props.getProperty(StreamType.ARCHIVE.getPropertyKey(RAW_PREFIX));
    BatchOptions opts = new BatchOptions();
    opts.setProps(props);
    setFirehose(props);
    SparkLambdaKinesisConnector connector =
      new SparkLambdaKinesisConnector(output, opts, spark.jsc());
    connector.configure(null, source);
    run(connector, LambdaTestUtils.createRecords(1, 1));
  }

//  @Test
//  public void testtmp() throws Exception {
//    String uuid = "integration-test-" + System.currentTimeMillis() + "-";
//    Properties props = setProperties(uuid);
//
//    BatchOptions opts = new BatchOptions();
//    opts.setProps(props);
//    setFirehose(props);
//    SparkLambdaKinesisConnector connector =
//      new SparkLambdaKinesisConnector(output, opts, spark.jsc());
//    connector.configure(null, "bulk-load-source");
//    byte[] bytes = LambdaTestUtils.asBytes(LambdaTestUtils.createRecords(1, 1)[0]);
//    connector.write(bytes);
//  }

  private void setFirehose(Properties props) {
    Map<String, SparkFirehoseStreams.StreamLookup> map = new HashMap<>();
    // local streams that don't have an output
    for (String name : asList(
      props.getProperty(StreamType.ARCHIVE.getPropertyKey(RAW_PREFIX)),
      props.getProperty(StreamType.COMMIT_ERROR.getPropertyKey(RAW_PREFIX)),
      props.getProperty(StreamType.PROCESSING_ERROR.getPropertyKey(RAW_PREFIX)),
      props.getProperty(StreamType.COMMIT_ERROR.getPropertyKey(STAGED_PREFIX)),
      props.getProperty(StreamType.PROCESSING_ERROR.getPropertyKey(STAGED_PREFIX)))) {
      map.put(name,
        new SparkFirehoseStreams.StreamLookup("file", output.getRoot().getAbsolutePath()));

    }
    // remote streams
    for (String name : asList(
      props.getProperty(StreamType.ARCHIVE.getPropertyKey(STAGED_PREFIX)))) {
      map.put(name,
        new SparkFirehoseStreams.StreamLookup("s3", TestProperties.Firehose.S3_BUCKET_NAME));
    }

    this.firehoses = new SparkFirehoseStreams(2 * TestProperties.ONE_MINUTE, map);
  }

  @Override
  protected List<Module> getAdditionalModules() {
    return asList(new SingleInstanceModule<>(firehoses, FirehoseStreams.class));
  }
}
