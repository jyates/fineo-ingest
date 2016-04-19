package io.fineo.lambda.e2e;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.fineo.lambda.configure.FirehoseModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.kinesis.MockKinesisStreams;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.lambda.LocalLambdaLocalKinesisConnector;
import io.fineo.lambda.e2e.resources.manager.MockResourceManager;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.raw.RawRecordToAvroHandler;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static io.fineo.lambda.configure.SingleInstanceModule.instanceModule;
import static io.fineo.lambda.configure.legacy.LambdaClientProperties.RAW_PREFIX;
import static io.fineo.lambda.configure.legacy.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.legacy.LambdaClientProperties
  .getFirehoseStreamPropertyVisibleForTesting;
import static io.fineo.lambda.configure.legacy.StreamType.ARCHIVE;

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

    LambdaKinesisConnector connector = new LocalLambdaLocalKinesisConnector();
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    MockResourceManager manager = new MockResourceManager(connector, store);
    LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> start = ingestStage(props, store, manager);
    LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> storage = storageStage(props, store,
      manager);

    Map<String, List<IngestUtil.Lambda>> stageMap =
      IngestUtil.newBuilder()
                .local()
                .then(INGEST_CONNECTOR, start, getHandler(start))
                .then(STAGE_CONNECTOR, storage, getHandler(storage))
                .build();
    connector.configure(stageMap, INGEST_CONNECTOR);

    EndToEndTestRunner runner =
      new EndToEndTestRunner(
        LambdaClientProperties.create(new PropertiesModule(props), instanceModule(props)), manager);

    return new TestState(stageMap, runner, manager);
  }

  private static Function<KinesisEvent, ?> getHandler(LambdaWrapper<KinesisEvent, ?> lambda) {
    return event -> {
      try {
        lambda.handle(event);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    };
  }

  private static LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> ingestStage(Properties
    props,
    SchemaStore store, MockResourceManager manager) throws IOException {
    NamedProvider module = new NamedProvider();
    module.add(FirehoseModule.FIREHOSE_ARCHIVE_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(LambdaClientProperties.RAW_PREFIX, StreamType.ARCHIVE));
    module.add(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(LambdaClientProperties.RAW_PREFIX, StreamType.COMMIT_ERROR));
    module.add(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(LambdaClientProperties.RAW_PREFIX, StreamType.PROCESSING_ERROR));
    return new LambdaWrapper<>(RawRecordToAvroHandler.class,
      new PropertiesModule(props),
      instanceModule(store),
      instanceModule(new MockKinesisStreams()),
      instanceModule(manager),
      module, new MockKinesisStreamsModule());
  }

  private static LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> storageStage(Properties
    props,
    SchemaStore store, MockResourceManager manager) throws IOException {
    NamedProvider module = new NamedProvider();
    module.add(FirehoseModule.FIREHOSE_ARCHIVE_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(LambdaClientProperties.STAGED_PREFIX, StreamType.ARCHIVE));
    module.add(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(LambdaClientProperties.STAGED_PREFIX, StreamType.COMMIT_ERROR));
    module.add(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(LambdaClientProperties.STAGED_PREFIX, StreamType.PROCESSING_ERROR));
    return new LambdaWrapper<>(RawRecordToAvroHandler.class,
      new PropertiesModule(props),
      instanceModule(store),
      instanceModule(new MockKinesisStreams()),
      instanceModule(manager),
      module, new MockKinesisStreamsModule());
  }

  private static class MockKinesisStreamsModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    @Inject
    public KinesisProducer producer(MockKinesisStreams streams) {
      return streams.getProducer();
    }

    @Provides
    @Inject
    public AvroToDynamoWriter dynamo(MockResourceManager manager) {
      return manager.getDynamo();
    }
  }

  private static class NamedProvider extends AbstractModule {

    private Map<String, Pair<Class, Provider>> map = new HashMap<>();

    @Override
    protected void configure() {
      for (Map.Entry<String, Pair<Class, Provider>> entry : map.entrySet()) {
        Class clazz = entry.getValue().getKey();
        Provider provider = entry.getValue().getValue();
        bind(clazz).annotatedWith(Names.named(entry.getKey())).toProvider(provider);
      }
    }

    public <T> void add(String name, Class<T> clazz, Provider<T> provider) {
      this.map.put(name, new ImmutablePair<>(clazz, provider));
    }
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

  public static class TestState2 {
    private Map<String, List<IngestUtil.Lambda>> stageMap;
    private EndToEndTestRunner2 runner;
    private ResourceManager resources;

    public TestState2(Map<String, List<IngestUtil.Lambda>> stageMap, EndToEndTestRunner2 runner,
      ResourceManager resources) {
      this.stageMap = stageMap;
      this.runner = runner;
      this.resources = resources;
    }

    public Map<String, List<IngestUtil.Lambda>> getStageMap() {
      return stageMap;
    }

    public EndToEndTestRunner2 getRunner() {
      return runner;
    }

    public ResourceManager getResources() {
      return resources;
    }
  }
}
