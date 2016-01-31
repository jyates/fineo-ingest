package io.fineo.lambda.e2e;

import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.e2e.resources.manager.MockResourceManager;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static io.fineo.lambda.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.StreamType.ARCHIVE;
import static io.fineo.lambda.LambdaClientProperties.getFirehoseStreamPropertyVisibleForTesting;

/**
 * Test the end-to-end workflow of the lambda architecture.
 */
public class TestEndToEndLambdaLocal {

  /**
   * Path where there are no issues with records.
   *
   * @throws Exception on failure
   */
  @Test
  public void testHappyPath() throws Exception {
    Properties props = new Properties();
    // firehose outputs
    props.setProperty(getFirehoseStreamPropertyVisibleForTesting(RAW_PREFIX, ARCHIVE), "raw-archived");
    props.setProperty(getFirehoseStreamPropertyVisibleForTesting(STAGED_PREFIX, ARCHIVE), "staged-archive");

    // between stage stream
    props.setProperty(LambdaClientProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME,
      "kinesis-parsed-records");

    EndToEndTestRunner runner =
      new EndToEndTestRunner(new LambdaClientProperties(props), new MockResourceManager());

    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    runner.run(json);

    runner.validate();

    runner.cleanup();
  }
}