package io.fineo.lambda.e2e;

import io.fineo.lambda.LambdaAvroToStorage;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.LambdaRawRecordToAvro;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.lambda.LocalLambdaLocalKinesisConnector;
import io.fineo.lambda.e2e.resources.lambda.LocalLambdaRemoteKinesisConnector;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.e2e.resources.manager.MockResourceManager;
import org.junit.Test;

import java.util.List;
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

  private static final String INGEST_CONNECTOR = "kinesis-ingest-records";
  private static final String STAGE_CONNECTOR = "kinesis-parsed-records";

  /**
   * Path where there are no issues with records.
   *
   * @throws Exception on failure
   */
  @Test
  public void testHappyPath() throws Exception {
    Properties props = new Properties();
    // firehose outputs
    props
      .setProperty(getFirehoseStreamPropertyVisibleForTesting(RAW_PREFIX, ARCHIVE), "raw-archived");
    props.setProperty(getFirehoseStreamPropertyVisibleForTesting(STAGED_PREFIX, ARCHIVE),
      "staged-archive");

    // between stage stream
    props.setProperty(LambdaClientProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME,
      STAGE_CONNECTOR);

    LambdaRawRecordToAvro start = new LambdaRawRecordToAvro();
    LambdaAvroToStorage storage = new LambdaAvroToStorage();
    Map<String, List<IngestUtil.Lambda>> stageMap =
      IngestUtil.newBuilder()
                .start(INGEST_CONNECTOR, start)
                .then(STAGE_CONNECTOR, storage)
                .build();
    LambdaKinesisConnector connector =
      new LocalLambdaLocalKinesisConnector(stageMap, INGEST_CONNECTOR);
    EndToEndTestRunner runner =
      new EndToEndTestRunner(new LambdaClientProperties(props),
        new MockResourceManager(connector, start, storage));

    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    runner.run(json);

    runner.validate();

    runner.cleanup();
  }
}
