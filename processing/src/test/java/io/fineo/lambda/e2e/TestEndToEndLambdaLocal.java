package io.fineo.lambda.e2e;

import io.fineo.lambda.LambdaAvroToStorage;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.LambdaRawRecordToAvro;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.lambda.LocalLambdaLocalKinesisConnector;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.e2e.resources.manager.MockResourceManager;
import io.fineo.lambda.util.ResourceManager;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.fineo.lambda.configure.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties.StreamType.ARCHIVE;
import static io.fineo.lambda.configure.LambdaClientProperties.getFirehoseStreamPropertyVisibleForTesting;

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
    runTest().getRunner().cleanup();
  }

  public static TestState runTest() throws Exception {
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
                .local()
                .start(INGEST_CONNECTOR, start)
                .then(STAGE_CONNECTOR, storage)
                .build();
    LambdaKinesisConnector connector =
      new LocalLambdaLocalKinesisConnector(stageMap, INGEST_CONNECTOR);
    ResourceManager manager = new MockResourceManager(connector, start, storage);
    EndToEndTestRunner runner = new EndToEndTestRunner(new LambdaClientProperties(props), manager);

    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    runner.run(json);
    runner.validate();

    return new TestState(stageMap, runner, manager);
  }

  public static class TestState {
    private Map<String, List<IngestUtil.Lambda>> stageMap;
    private EndToEndTestRunner runner;
    private ResourceManager resources;

    public TestState(Map<String, List<IngestUtil.Lambda>> stageMap, EndToEndTestRunner runner,
      ResourceManager resources) {
      this.stageMap = stageMap;
      this.runner = runner;
      this.resources = resources;
    }

    public Map<String, List<IngestUtil.Lambda>> getStageMap() {
      return stageMap;
    }

    public EndToEndTestRunner getRunner() {
      return runner;
    }

    public ResourceManager getResources() {
      return resources;
    }
  }
}
