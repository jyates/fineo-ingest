package io.fineo.stream.processing.e2e.command;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.beust.jcommander.Parameters;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.e2e.aws.dynamo.DelegateAwsDynamoResource;
import io.fineo.lambda.e2e.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.local.LocalFirehoseStreams;
import io.fineo.lambda.e2e.local.LocalLambdaLocalKinesisConnector;
import io.fineo.lambda.e2e.local.MockKinesisStreams;
import io.fineo.lambda.e2e.manager.IKinesisStreams;
import io.fineo.lambda.e2e.manager.ManagerBuilder;
import io.fineo.lambda.e2e.state.E2ETestState;
import io.fineo.lambda.e2e.state.EndToEndTestBuilder;
import io.fineo.lambda.e2e.state.EndToEndTestRunner;
import io.fineo.lambda.e2e.util.IngestUtil;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.raw.RawRecordToAvroHandler;
import io.fineo.lambda.handle.raw.RawStageWrapper;
import io.fineo.lambda.handle.staged.AvroToStorageHandler;
import io.fineo.lambda.handle.staged.AvroToStorageWrapper;
import io.fineo.lambda.handle.util.HandlerUtils;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.schema.store.SchemaStore;
import io.fineo.stream.processing.e2e.module.FakeAwsCredentialsModule;
import io.fineo.stream.processing.e2e.options.FirehoseOutput;
import io.fineo.stream.processing.e2e.options.LocalOptions;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
import static io.fineo.lambda.e2e.ITEndToEndLambdaLocal.INGEST_CONNECTOR;
import static io.fineo.lambda.e2e.ITEndToEndLambdaLocal.STAGE_CONNECTOR;
import static io.fineo.lambda.e2e.ITEndToEndLambdaLocal.getMockFirehoses;


/**
 * Run the pipeline almost entirely in-memory. Only the schema store is run externally so we can
 * maintain schema across processes via a LocalDynamoDB instance. Thus, the output from the storage
 * stage goes away as soon as this process goes away.
 * <p>
 * We do retain the output from firehose into an external file so we can test the follow on batch
 * stages.
 * </p>
 */
@Parameters(commandNames = "local",
            commandDescription = "Run the ingest against a local target")
public class InMemoryExecCommand extends BaseCommand {

  private static final String STORAGE_OUTPUT_STREAM = "staged-archive";

  private final FirehoseOutput output;
  private final LocalOptions local;

  public InMemoryExecCommand(LocalOptions local, FirehoseOutput output) {
    this.local = local;
    this.output = output;
  }

  @Override
  public void run(List<Module> baseModules, Map<String, Object> event) throws Exception {
    // setup the different lambda functions
    E2ETestState state = buildState(baseModules);
    EndToEndTestRunner runner = state.getRunner();
    runner.setup();
    runner.send(event);
    runner.validate();

    // write the output to the target file
    List<ByteBuffer> writes = state.getResources().getFirehoseWrites(STORAGE_OUTPUT_STREAM);
    File out = new File(output.get());
    FileChannel channel = new FileOutputStream(out, false).getChannel();
    for (ByteBuffer buff : writes) {
      buff.flip();
      channel.write(buff);
    }
    channel.close();
    runner.cleanup();
  }

  private E2ETestState buildState(List<Module> store) throws Exception {
    // need to set these here so the lambdaclientproperties is happy and work ok
    Properties props = new Properties();
    // firehose outputs
    props
      .setProperty(getFirehoseStreamPropertyVisibleForTesting(RAW_PREFIX, ARCHIVE), "raw-archived");
    props.setProperty(getFirehoseStreamPropertyVisibleForTesting(STAGED_PREFIX, ARCHIVE),
      STORAGE_OUTPUT_STREAM);

    // between stage stream
    props.setProperty(KINESIS_PARSED_RAW_OUT_STREAM_NAME, STAGE_CONNECTOR);

    ManagerBuilder managerBuilder = new ManagerBuilder();
    managerBuilder.withStore(null);
    managerBuilder.withAdditionalModules(store);
    Module credentials = new FakeAwsCredentialsModule();
    managerBuilder.withAwsCredentials(credentials);
    MockKinesisStreams streams = new MockKinesisStreams();
    managerBuilder.withStreams(streams);
    LocalFirehoseStreams firehoses = new LocalFirehoseStreams();
    managerBuilder.withFirehose(firehoses);
    LambdaKinesisConnector connector = new LocalLambdaLocalKinesisConnector();
    managerBuilder.withConnector(connector);
    DelegateAwsDynamoResource
      .addLocalDynamo(managerBuilder, local.host, local.port, local.ingestTablePrefix);

    // modules for the stages
    List<Module> baseModules = newArrayList(store);
    baseModules.add(new SingleInstanceModule<>(streams, IKinesisStreams.class));
    baseModules.add(credentials);
    baseModules.add(new PropertiesModule(props));
    baseModules.add(new Module() {
      @Override
      public void configure(Binder binder) {
      }

      @Provides
      @Inject
      public IKinesisProducer producer(IKinesisStreams streams) {
        return streams.getProducer();
      }

    });

    // ingest
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

    EndToEndTestBuilder builder = new EndToEndTestBuilder(managerBuilder, props);
    EndToEndTestRunner runner = builder.validateRawPhase(25).all()
                                       .validateStoragePhase().all()
                                       .build();
    return new E2ETestState(runner);
  }

  private SchemaStore getSchemaStore(List<Module> baseModules) {
    List<Module> schema = newArrayList(baseModules);
    Injector guice = Guice.createInjector(schema);
    return guice.getInstance(SchemaStore.class);
  }
}
