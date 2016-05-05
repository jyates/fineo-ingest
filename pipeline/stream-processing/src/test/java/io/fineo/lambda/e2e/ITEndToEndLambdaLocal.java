package io.fineo.lambda.e2e;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
import io.fineo.lambda.e2e.resources.kinesis.MockKinesisStreams;
import io.fineo.lambda.e2e.resources.lambda.LocalLambdaLocalKinesisConnector;
import io.fineo.lambda.e2e.resources.manager.MockResourceManager;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.raw.RawRecordToAvroHandler;
import io.fineo.lambda.handle.raw.RawStageWrapper;
import io.fineo.lambda.handle.staged.AvroToStorageHandler;
import io.fineo.lambda.handle.staged.AvroToStorageWrapper;
import io.fineo.lambda.handle.util.HandlerUtils;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.etl.FineoProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME;
import static io.fineo.etl.FineoProperties.RAW_PREFIX;
import static io.fineo.etl.FineoProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.legacy.LambdaClientProperties
  .getFirehoseStreamPropertyVisibleForTesting;
import static io.fineo.lambda.configure.legacy.StreamType.ARCHIVE;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;

/**
 * Test the end-to-end workflow of the lambda architecture.
 */
public class ITEndToEndLambdaLocal {

  private static final Logger LOG = LoggerFactory.getLogger(ITEndToEndLambdaLocal.class);
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

    LOG.info("**** Starting second run ******");
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
    props.setProperty(KINESIS_PARSED_RAW_OUT_STREAM_NAME,
      STAGE_CONNECTOR);

    LambdaKinesisConnector connector = new LocalLambdaLocalKinesisConnector();
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    MockKinesisStreams streams = new MockKinesisStreams();
    MockResourceManager manager = new MockResourceManager(connector, store, streams);
    LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> start = ingestStage(props, store, manager);
    LambdaWrapper<KinesisEvent, AvroToStorageHandler> storage = storageStage(props, store, manager);

    Map<String, List<IngestUtil.Lambda>> stageMap =
      IngestUtil.newBuilder()
                .then(INGEST_CONNECTOR, start, HandlerUtils.getHandler(start))
                .then(STAGE_CONNECTOR, storage, HandlerUtils.getHandler(storage))
                .build();
    connector.configure(stageMap, INGEST_CONNECTOR);

    EndToEndTestRunner runner = new EndToEndTestBuilder(manager, props).validateAll().build();
    return new TestState(stageMap, runner, manager);
  }

  private static LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> ingestStage(Properties
    props,
    SchemaStore store, MockResourceManager manager) throws IOException {
    NamedProvider module = new NamedProvider();
    module.add(FirehoseModule.FIREHOSE_ARCHIVE_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(RAW_PREFIX, StreamType.ARCHIVE));
    module.add(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(RAW_PREFIX, StreamType.COMMIT_ERROR));
    module.add(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(RAW_PREFIX, StreamType.PROCESSING_ERROR));
    List<Module> modules = getBaseModules(props, store, manager);
    modules.add(module);
    return new RawStageWrapper(modules);
  }

  private static LambdaWrapper<KinesisEvent, AvroToStorageHandler> storageStage(Properties
    props, SchemaStore store, MockResourceManager manager) throws IOException {
    NamedProvider module = new NamedProvider();
    module.add(FirehoseModule.FIREHOSE_ARCHIVE_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(FineoProperties.STAGED_PREFIX, StreamType.ARCHIVE));
    module.add(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(FineoProperties.STAGED_PREFIX, StreamType.COMMIT_ERROR));
    module.add(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM, FirehoseBatchWriter.class,
      () -> manager.getWriter(FineoProperties.STAGED_PREFIX, StreamType.PROCESSING_ERROR));
    List<Module> modules = getBaseModules(props, store, manager);
    modules.add(module);
    return new AvroToStorageWrapper(modules);
  }

  private static List<Module> getBaseModules(Properties
    props, SchemaStore store, MockResourceManager manager) {
    return newArrayList(new PropertiesModule(props),
      instanceModule(store),
      new SingleInstanceModule<>(manager.getStreams(), IKinesisStreams.class),
      instanceModule(manager),
      new LazyMockComponents());
  }

  private static class LazyMockComponents extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    @Inject
    public IKinesisProducer producer(IKinesisStreams streams) {
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
}
