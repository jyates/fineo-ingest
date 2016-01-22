package io.fineo.lambda;

import io.fineo.lambda.util.EndToEndTestRunner;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.mock.MockResourceManager;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static io.fineo.lambda.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.StreamType.ARCHIVE;
import static io.fineo.lambda.LambdaClientProperties.StreamType.PROCESSING_ERROR;
import static io.fineo.lambda.LambdaClientProperties.getFirehoseStreamProperty;

/**
 * Test the end-to-end workflow of the lambda architecture.
 */
public class TestEndToEndLambda {

  /**
   * Path where there are no issues with records.
   *
   * @throws Exception on failure
   */
  @Test
  public void testHappyPath() throws Exception {
    // Setup
    //-------
    Properties props = new Properties();
    // firehose outputs
    props.setProperty(getFirehoseStreamProperty(RAW_PREFIX, ARCHIVE), "raw-archived");
    props.setProperty(getFirehoseStreamProperty(STAGED_PREFIX, ARCHIVE), "staged-archive");

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