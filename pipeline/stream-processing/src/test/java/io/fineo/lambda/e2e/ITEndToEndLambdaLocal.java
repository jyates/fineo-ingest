package io.fineo.lambda.e2e;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.HandlerSetupUtils;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.configure.StreamType;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.local.LocalFirehoseStreams;
import io.fineo.lambda.e2e.local.LocalLambdaLocalKinesisConnector;
import io.fineo.lambda.e2e.local.MockAvroToDynamo;
import io.fineo.lambda.e2e.local.MockKinesisStreams;
import io.fineo.lambda.e2e.manager.IDynamoResource;
import io.fineo.lambda.e2e.manager.IFirehoseResource;
import io.fineo.lambda.e2e.manager.IKinesisStreams;
import io.fineo.lambda.e2e.manager.ManagerBuilder;
import io.fineo.lambda.e2e.state.E2ETestState;
import io.fineo.lambda.e2e.state.EndToEndTestBuilder;
import io.fineo.lambda.e2e.state.EndToEndTestRunner;
import io.fineo.lambda.e2e.util.IngestUtil;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.MalformedEventToJson;
import io.fineo.lambda.handle.raw.RawRecordToAvroHandler;
import io.fineo.lambda.handle.raw.RawStageWrapper;
import io.fineo.lambda.handle.staged.AvroToStorageHandler;
import io.fineo.lambda.handle.staged.AvroToStorageWrapper;
import io.fineo.lambda.handle.util.HandlerUtils;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import io.fineo.schema.timestamp.MultiPatternTimestampParser;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Spliterators;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static io.fineo.etl.FineoProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME;
import static io.fineo.etl.FineoProperties.RAW_PREFIX;
import static io.fineo.etl.FineoProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.LambdaClientProperties
  .getFirehoseStreamPropertyVisibleForTesting;
import static io.fineo.lambda.configure.StreamType.ARCHIVE;

/**
 * Test the end-to-end workflow of the lambda architecture.
 */
public class ITEndToEndLambdaLocal {

  private static final Logger LOG = LoggerFactory.getLogger(ITEndToEndLambdaLocal.class);
  public static final String INGEST_CONNECTOR = "kinesis-ingest-records";
  public static final String STAGE_CONNECTOR = "kinesis-parsed-records";

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
    E2ETestState state = runTest(event);
    state.getRunner().cleanup();

    LOG.info("**** Starting second run ******");
    Map<String, Object> second = new HashMap<>(event);
    second.put("anotherField", 1);
    run(state, second);
    state.getRunner().cleanup();
  }

  @Test
  public void testCustomMetricKey() throws Exception {
    String org = "orgid", metric = "metricname", field = "fieldname";
    String customKey = "somekey";

    // create the schema for the org
    E2ETestState state = prepareTest();
    SchemaStore store = state.getResources().getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(org)
           .withMetricKeys(customKey)
           .newMetric().setDisplayName(metric)
           .newField().withName(field).withType(StoreManager.Type.BOOLEAN).build()
           .build().commit();

    // simple event
    Map<String, Object> event = new HashMap<>();
    event.put(AvroSchemaProperties.ORG_ID_KEY, org);
    event.put(customKey, metric);
    event.put(AvroSchemaProperties.TIMESTAMP_KEY, 1234);
    event.put(field, true);

    // do the actual running
    run(state, event, false);
    state.getRunner().cleanup();
  }

  @Test
  public void testTestCustomTimestampAliasAndFormat() throws Exception {
    final String fixedTsString = "Tue, 06 Sep 2016 19:00:46 GMT";
    final long fixedTs = 1473188446000l;

    String timestampAlias = "tsAlias";
    String org = "orgid", metric = "metricname", field = "fieldname";

    // create the schema for the org
    E2ETestState state = prepareTest();
    SchemaStore store = state.getResources().getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(org)
           .newMetric().setDisplayName(metric)
           .withTimestampFormat(MultiPatternTimestampParser.TimeFormats.RFC_1123_DATE_TIME.name())
           .newField().withName(field).withType(StoreManager.Type.BOOLEAN).build()
           .build().commit();
    manager.updateOrg(org)
           .updateMetric(metric).addFieldAlias(AvroSchemaProperties.TIMESTAMP_KEY, timestampAlias)
           .build().commit();

    Map<String, Object> event = new HashMap<>();
    event.put(AvroSchemaProperties.ORG_ID_KEY, org);
    event.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metric);
    event.put(timestampAlias, fixedTsString);
    event.put(field, true);

    Map<String, Object> expectedOut = newHashMap(event);
    expectedOut.remove(timestampAlias);
    expectedOut.put(AvroSchemaProperties.TIMESTAMP_KEY, fixedTs);

    run(state, event, expectedOut, false);
    state.getRunner().cleanup();
  }

  @Test
  public void testMultipleEvents() throws Exception {
    Map<String, Object>[] events = LambdaTestUtils.createRecordsForSingleTenant(3, 2,
      (recordCount, fieldName) -> {
        String index = fieldName.substring(1);
        switch (index) {
          case "0":
            return false;
          case "1":
            return recordCount;
          default:
            throw new RuntimeException("Should only have two different fields!");
        }
      });
    E2ETestState state = prepareTest();

    EndToEndTestRunner runner = state.getRunner();
    runner.setup();
    for (int i = 0; i < events.length; i++) {
      if (i == 0) {
        runner.run(events[i]);
      } else {
        runner.send(events[i]);
      }
    }
    runner.validate();
    state.getRunner().cleanup();
  }

  public static E2ETestState runTest() throws Exception {
    return runTest(LambdaTestUtils.createRecords(1, 1)[0]);
  }

  public static E2ETestState runTest(Map<String, Object> json) throws Exception {
    E2ETestState state = prepareTest();
    run(state, json);
    return state;
  }

  public static E2ETestState prepareTest() throws Exception {
    return prepareTest(new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY)));
  }

  public static E2ETestState prepareTest(SchemaStore store) throws Exception {
    ManagerBuilder builder = new ManagerBuilder();
    builder.withStore(store);
    return prepareTest(builder);
  }

  public static E2ETestState prepareTest(ManagerBuilder builder) throws Exception {
    Properties props = new Properties();
    // firehose outputs
    props
      .setProperty(getFirehoseStreamPropertyVisibleForTesting(RAW_PREFIX, ARCHIVE), "raw-archived");
    props.setProperty(getFirehoseStreamPropertyVisibleForTesting(STAGED_PREFIX, ARCHIVE),
      "staged-archive");

    // between stage stream
    props.setProperty(KINESIS_PARSED_RAW_OUT_STREAM_NAME, STAGE_CONNECTOR);
    props.setProperty(FineoProperties.DYNAMO_TABLE_MANAGER_CACHE_TIME, "3600000");

    LambdaKinesisConnector connector = new LocalLambdaLocalKinesisConnector();

    MockKinesisStreams streams = new MockKinesisStreams();
    builder.withStreams(streams);
    MockAvroToDynamo dynamo = new MockAvroToDynamo();
    builder.withDynamo(dynamo);
    LocalFirehoseStreams firehoses = new LocalFirehoseStreams();
    builder.withFirehose(firehoses);
    builder.withConnector(connector);
    List<Module> baseModules = getBaseModules(props, dynamo, streams);
    // start
    List<Module> rawStage = newArrayList(baseModules);
    rawStage.add(getMockFirehoses(firehoses, RAW_PREFIX));
    LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> start = new RawStageWrapper(rawStage);
    // storage
    List<Module> writeStage = newArrayList(baseModules);
    writeStage.add(getMockFirehoses(firehoses, STAGED_PREFIX));
    LambdaWrapper<KinesisEvent, AvroToStorageHandler> storage =
      new AvroToStorageWrapper(writeStage);

    Map<String, List<IngestUtil.Lambda>> stageMap =
      IngestUtil.newBuilder()
                .then(INGEST_CONNECTOR, start, HandlerUtils.getHandler(start))
                .then(STAGE_CONNECTOR, storage, HandlerUtils.getHandler(storage))
                .build();
    connector.configure(stageMap, INGEST_CONNECTOR);

    EndToEndTestRunner runner = new EndToEndTestBuilder(builder, props).validateAll().build();
    addStoreModule(builder, runner, rawStage, writeStage);
    E2ETestState state = new E2ETestState(runner);
    state.setStages(start, storage);
    return state;
  }

  private static void addStoreModule(ManagerBuilder builder, EndToEndTestRunner runner,
    List<Module>... stages) {
    // kind of a hack around the schema store. Generally, we will have the schema store passed
    // into the builder. However, in some cases, we actually want an external schema store, but
    // don't want to deal with instantiating it and then passing it in, so we lazily load it from
    // the runner's manager. This has to happen _after_ the runner calls manager#setup so things
    // are initialized
    Module store;
    if (builder.getStore() != null) {
      store = new SingleInstanceModule<>(builder.getStore(), SchemaStore.class);
    } else {
      store = new Module() {
        @Override
        public void configure(Binder binder) {
        }

        @Provides
        public SchemaStore store() {
          return runner.getManager().getStore();
        }
      };
    }

    for (List<Module> stage : stages) {
      stage.add(store);
    }
  }

  public static Module getMockFirehoses(IFirehoseResource firehoses, String stagePrefix) {
    NamedProvider module = new NamedProvider();
    module.add(FirehoseModule.FIREHOSE_ARCHIVE_STREAM, IFirehoseBatchWriter.class,
      () -> firehoses.getWriter(stagePrefix, StreamType.ARCHIVE));
    module.add(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM, IFirehoseBatchWriter.class,
      () -> firehoses.getWriter(stagePrefix, StreamType.COMMIT_ERROR));
    module.add(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM, IFirehoseBatchWriter.class,
      () -> firehoses.getWriter(stagePrefix, StreamType.PROCESSING_ERROR));
    return module;
  }

  private static List<Module> getBaseModules(Properties props,
    MockAvroToDynamo dynamo, MockKinesisStreams streams) {
    return newArrayList(new PropertiesModule(props),
      new SingleInstanceModule<>(dynamo, IDynamoResource.class),
      new SingleInstanceModule<>(streams, IKinesisStreams.class),
      new LazyMockComponents(),
      HandlerSetupUtils.clockModule(),
      MalformedEventToJson.getModule());
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
    public AvroToDynamoWriter dynamo(IDynamoResource dynamo) {
      return dynamo.getWriter();
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

  public static void run(E2ETestState state, Map<String, Object> json) throws Exception {
    run(state, json, true);
  }

  public static void run(E2ETestState state, Map<String, Object> json, boolean registerEvent)
    throws Exception {
    run(state, json, json, registerEvent);
  }

  public static void run(E2ETestState state, Map<String, Object> json, Map<String, Object>
    expected, boolean registerEvent)
    throws Exception {
    EndToEndTestRunner runner = state.getRunner();
    runner.setup();
    if (registerEvent) {
      runner.run(json);
    } else {
      runner.send(json, expected);
    }
    runner.validate();
  }
}
