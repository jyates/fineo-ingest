package io.fineo.lambda.e2e;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.fineo.lambda.LambdaAvroToStorage;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.LambdaRawRecordToAvro;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.lambda.LocalLambdaLocalKinesisConnector;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.e2e.resources.manager.MockResourceManager;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.store.SchemaStore;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.fineo.lambda.configure.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties.StreamType.ARCHIVE;
import static io.fineo.lambda.configure.LambdaClientProperties
  .getFirehoseStreamPropertyVisibleForTesting;

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

  @Test
  public void testChangingSchemaForSecondEvent() throws Exception {
    Map<String, Object> event = LambdaTestUtils.createRecords(1, 1)[0];
    TestState state = runTest(event);
    state.getRunner().cleanup();

    Map<String, Object> second = new HashMap<>(event);
    second.put("anotherField", 1);
    run(state, second);
  }

  public static TestState runTest() throws Exception {
    return runTest(LambdaTestUtils.createRecords(1, 1)[0]);
  }

  public static TestState runTest(Map<String, Object> json) throws Exception {
    TestState state = prepareTest();
    run(state, json);
    return state;
  }

  public static TestState prepareTest() throws Exception {
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
    EndToEndTestRunner runner = new EndToEndTestRunner(LambdaClientProperties.create(
      new PropertiesModule(props), new AbstractModule() {
        @Override
        protected void configure() {
        }

        @Provides
        @Singleton
        public SchemaStore getStore() {
          return new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
        }
      }), manager);

    return new TestState(stageMap, runner, manager);
  }


  public static void run(TestState state, Map<String, Object> json) throws Exception {
    EndToEndTestRunner runner = state.getRunner();
    runner.setup();
    runner.run(json);
    runner.validate();
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
