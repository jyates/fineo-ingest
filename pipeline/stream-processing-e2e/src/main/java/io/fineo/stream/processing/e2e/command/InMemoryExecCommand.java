package io.fineo.stream.processing.e2e.command;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.beust.jcommander.Parameters;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.lambda.e2e.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.local.LocalLambdaLocalKinesisConnector;
import io.fineo.lambda.e2e.local.MockKinesisStreams;
import io.fineo.lambda.e2e.state.E2ETestState;
import io.fineo.lambda.e2e.state.EndToEndTestBuilder;
import io.fineo.lambda.e2e.state.EndToEndTestRunner;
import io.fineo.lambda.e2e.util.IngestUtil;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.raw.RawRecordToAvroHandler;
import io.fineo.lambda.handle.staged.AvroToStorageHandler;
import io.fineo.lambda.handle.util.HandlerUtils;
import io.fineo.schema.store.SchemaStore;
import io.fineo.stream.processing.e2e.options.FirehoseOutput;

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


/**
 * Run the pipeline almost entirely in-memory. Only the schema store is run externally so we can
 * maintain schema across processes via a LocalDynamoDB instance. Thus, the output from the storage
 * stage goes away as soon as this process goes away.
 * <p>
 *  We do retain the output from firehose into an external file so we can test the follow on batch
 *  stages.
 * </p>
 */
@Parameters(commandNames = "local",
            commandDescription = "Run the ingest against a local target")
public class InMemoryExecCommand extends BaseCommand {

  private static final String STORAGE_OUTPUT_STREAM = "staged-archive";

  private final FirehoseOutput output;

  public InMemoryExecCommand(FirehoseOutput output) {
    this.output = output;
  }

  @Override
  public void run(List<Module> baseModules, Map<String, Object> event) throws Exception {
    // setup the different lambda functions
    SchemaStore store = getSchemaStore(baseModules);
    E2ETestState state = buildState(store);
    EndToEndTestRunner runner = state.getRunner();
    runner.setup();
    runner.send(event);
    runner.validate();

    // write the output to the target file
    List<ByteBuffer> writes = state.getResources().getFirehoseWrites(STORAGE_OUTPUT_STREAM);
    File out = new File(output.get());
    FileChannel channel = new FileOutputStream(out, false).getChannel();
    for(ByteBuffer buff: writes) {
      buff.flip();
      channel.write(buff);
    }
    channel.close();
    runner.cleanup();
  }

  private E2ETestState buildState(SchemaStore store) throws Exception {
    Properties props = new Properties();
    // firehose outputs
    props
      .setProperty(getFirehoseStreamPropertyVisibleForTesting(RAW_PREFIX, ARCHIVE), "raw-archived");
    props.setProperty(getFirehoseStreamPropertyVisibleForTesting(STAGED_PREFIX, ARCHIVE),
      STORAGE_OUTPUT_STREAM);

    // between stage stream
    props.setProperty(KINESIS_PARSED_RAW_OUT_STREAM_NAME, STAGE_CONNECTOR);

    LambdaKinesisConnector connector = new LocalLambdaLocalKinesisConnector();

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

    EndToEndTestBuilder builder = new EndToEndTestBuilder(manager, props);
    EndToEndTestRunner runner = builder.validateRawPhase(25).all()
                                       .validateStoragePhase().all()
                                       .build();
    return new E2ETestState(runner, manager);
  }

  private SchemaStore getSchemaStore(List<Module> baseModules) {
    List<Module> schema = newArrayList(baseModules);
    Injector guice = Guice.createInjector(schema);
    return guice.getInstance(SchemaStore.class);
  }
}
