package io.fineo.lambda.e2e.resources.manager;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.e2e.EndtoEndSuccessStatus;
import io.fineo.lambda.e2e.resources.ResourceUtils;
import io.fineo.lambda.e2e.resources.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.manager.collector.OutputCollector;
import io.fineo.lambda.util.IResourceManager;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static io.fineo.etl.FineoProperties.RAW_PREFIX;
import static io.fineo.etl.FineoProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static java.lang.String.format;

public class ResourceManager implements IResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManager.class);
  private static final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
    MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));

  private final Injector injector;
  private final OutputCollector output;
  private final boolean cleanup;

  protected LambdaKinesisConnector connector;
  private IFirehoseResource firehose;
  private IKinesisStreams kinesis;
  private IDynamoResource dynamo;

  public ResourceManager(List<Module> modules, OutputCollector output, boolean cleanup) {
    FutureWaiter future = new FutureWaiter(executor);
    modules.add(instanceModule(future));
    this.injector = Guice.createInjector(modules);
    this.output = output;
    this.cleanup = cleanup;
  }

  @Override
  public void setup() throws Exception {
    FutureWaiter future = this.injector.getInstance(FutureWaiter.class);
    this.connector = injector.getInstance(LambdaKinesisConnector.class);
    this.firehose = get(IFirehoseResource.class);
    this.kinesis = get(IKinesisStreams.class);
    this.dynamo = get(IDynamoResource.class);

    future.run(() -> {
      try {
        connector.connect(this.kinesis);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    future.await();
  }

  private <T extends IResource> T get(Class<T> clazz) throws IOException {
    T inst = injector.getInstance(clazz);
    inst.init(injector);
    return inst;
  }

  @Override
  public byte[] send(Map<String, Object> json) throws Exception {
    byte[] start = LambdaTestUtils.asBytes(json);
    this.connector.write(start);
    return start;
  }

  @Override
  public void cleanup(EndtoEndSuccessStatus status) throws Exception {
    try {
      if (status != null && !status.isSuccessful()) {
        LOG.info(" ---- FAILURE ----");
        LOG.info("");
        LOG.info("Data available at: " + output.getRoot());
        LOG.info("");
        LOG.info(" ---- FAILURE ----");
        if (!status.isMessageSent() || !status.isUpdateStoreCorrect()) {
          LOG.error("We didn't start the test correctly!");
          firehose.ensureNoDataStored();
        } else {
          // just get all the data, maybe we messed up the test
          cloneS3(RAW_PREFIX);
          cloneS3(STAGED_PREFIX);

          // copy kinesis data
          OutputCollector out = output.getNextLayer("kinesis");
          for (String streamName : kinesis.getStreamNames()) {
            ResourceUtils.writeStream(streamName, out, () -> this.connector.getWrites(streamName));
          }
          // copy any dynamo data
          dynamo.copyStoreTables(output.getNextLayer(STAGED_PREFIX).getNextLayer("dynamo"));
        }
      }
    } finally {
      if (cleanup || status.isSuccessful())
        deleteResources();
    }
  }

  private void deleteResources() throws InterruptedException {
    FutureWaiter waiter = this.injector.getInstance(FutureWaiter.class);
    cleanup(waiter, this.firehose, this.kinesis, this.dynamo);
    // wait for all the resources to cleanup
    this.connector.cleanup(waiter);
    waiter.await();
  }

  private <T extends IResource> void cleanup(FutureWaiter waiter, T... resources) {
    for (T r : resources)
      r.cleanup(waiter);
  }

  private void cloneS3(String stage) throws IOException {
    List<Pair<String, StreamType>> toClone = new ArrayList<>(3);
    for (StreamType t : StreamType.values()) {
      LOG.debug(format("Cloning s3 [%s => %s]", stage, t));
      toClone.add(new Pair<>(stage, t));
    }
    OutputCollector dir = output.getNextLayer(stage).getNextLayer("s3");
    firehose.clone(toClone, dir);
  }

  @Override
  public List<ByteBuffer> getFirehoseWrites(String streamName) {
    return this.firehose.getFirehoseWrites(streamName);
  }

  @Override
  public BlockingQueue<List<ByteBuffer>> getKinesisWrites(String streamName) {
    return this.connector.getWrites(streamName);
  }

  @Override
  public SchemaStore getStore() {
    return injector.getInstance(SchemaStore.class);
  }

  @Override
  public void verifyDynamoWrites(RecordMetadata metadata, Map<String, Object> json) {
    dynamo.verify(metadata, json);
  }
}
