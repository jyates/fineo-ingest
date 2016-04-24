package io.fineo.lambda.e2e.resources.aws.manager;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.SchemaStoreModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.configure.util.InstanceToNamed;
import io.fineo.lambda.e2e.EndtoEndSuccessStatus;
import io.fineo.lambda.e2e.resources.aws.AwsResource;
import io.fineo.lambda.e2e.resources.ResourceUtils;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.aws.dynamo.DynamoResource;
import io.fineo.lambda.e2e.resources.aws.firehose.FirehoseResource;
import io.fineo.lambda.e2e.resources.aws.kinesis.KinesisStreamManager;
import io.fineo.lambda.e2e.resources.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.manager.BaseResourceManager;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import io.fineo.test.rule.TestOutput;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static io.fineo.lambda.e2e.validation.util.ValidationUtils.verifyRecordMatchesJson;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Manages lambda test resources on AWS
 */
public class AwsResourceManager extends BaseResourceManager {
  private static final Log LOG = LogFactory.getLog(AwsResourceManager.class);
  private static final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
    MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));

  private final String region;

  private final Module awsCredentials;
  private final TestOutput output;
  private final List<Module> modules;

  private FirehoseResource firehose;
  private KinesisStreamManager kinesis;
  private DynamoResource dynamo;
  private List<AwsResource> resources = new ArrayList<>();
  private boolean cleanup;

  public AwsResourceManager(Module awsCredentials, TestOutput output,
    LambdaKinesisConnector connector, String region, List<Module> additionalModules) {
    super(connector);
    this.awsCredentials = awsCredentials;
    this.output = output;
    this.region = region;
    this.modules = newArrayList(additionalModules);
  }

  /**
   * Cleanup the AWS resources in the case of failure
   */
  public void cleanupResourcesOnFailure(boolean cleanup) {
    this.cleanup = cleanup;
  }

  @Override
  public void setup(LambdaClientProperties props) throws Exception {
    FutureWaiter future = new FutureWaiter(executor);
    ResultWaiter.ResultWaiterFactory waiter = new ResultWaiter.ResultWaiterFactory(TestProperties
      .FIVE_MINUTES, TestProperties.ONE_SECOND);

    modules.addAll(newArrayList(
      awsCredentials,
      instanceModule(props),
      instanceModule(props.getRawPropertiesForTesting()),
      new PropertiesModule(props.getRawPropertiesForTesting()),
      instanceModule(waiter),
      new SchemaStoreModule(),
      new DynamoModule(),
      new DynamoRegionConfigurator(),
      new InstanceToNamed<>(FineoProperties.DYNAMO_REGION, region, String.class),
      new InstanceToNamed<>("aws.region", region, String.class),
      new InstanceToNamed<>("aws.kinesis.shard.count", TestProperties.Kinesis.SHARD_COUNT,
        Integer.class)
    ));
    Injector injector = Guice.createInjector(modules);

    // load all the things we will need
    this.dynamo = injector.getInstance(DynamoResource.class);
    this.firehose = injector.getInstance(FirehoseResource.class);
    this.kinesis = injector.getInstance(KinesisStreamManager.class);

    dynamo.setup(future);
    resources.add(dynamo);

    // setup the firehose connections
    for (String stage : TestProperties.Lambda.STAGES) {
      firehose.createFirehoses(stage, future);
    }
    resources.add(firehose);

    // setup the kinesis streams + interconnection
    future.run(() -> {
      try {
        connector.connect(this.kinesis);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    resources.add(kinesis);
    resources.add(connector);

    // wait for all the setup to complete
    future.await();
  }

  @Override
  public List<ByteBuffer> getFirehoseWrites(String stream) {
    return firehose.read(stream);
  }

  @Override
  public List<ByteBuffer> getKinesisWrites(String stream) {
    return this.connector.getWrites(stream);
  }

  @Override
  public SchemaStore getStore() {
    return this.dynamo.getStore();
  }

  @Override
  public void verifyDynamoWrites(RecordMetadata metadata, Map<String, Object> json) {
    List<GenericRecord> records = dynamo.read(metadata);
    assertEquals(newArrayList(records.get(0)), records);
    verifyRecordMatchesJson(getStore(), json, records.get(0));
  }

  @Override
  public void cleanup(EndtoEndSuccessStatus status) throws InterruptedException, IOException {
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
          cloneS3(FineoProperties.RAW_PREFIX);
          cloneS3(FineoProperties.STAGED_PREFIX);

          // copy kinesis data
          File out = output.newFolder("kinesis");
          for (String streamName : kinesis.getStreamNames()) {
            ResourceUtils.writeStream(streamName, out, () -> this.connector.getWrites(streamName));
          }
          // copy any dynamo data
          dynamo.copyStoreTables(output.newFolder(FineoProperties.STAGED_PREFIX, "dynamo"));
        }
      }
    } finally {
      if (cleanup || status.isSuccessful())
        deleteResources();
    }
  }

  private void deleteResources() throws InterruptedException {
    FutureWaiter futures = new FutureWaiter(executor);
    for (AwsResource resource : resources) {
      resource.cleanup(futures);
    }

    futures.await();
  }

  private void cloneS3(String stage) throws IOException {
    List<Pair<String, StreamType>> toClone = new ArrayList<>(3);
    for (StreamType t : StreamType.values()) {
      LOG.debug(format("Cloning s3 [%s => %s]", stage, t));
      toClone.add(new Pair<>(stage, t));
    }
    File dir = output.newFolder(stage, "s3");
    firehose.clone(toClone, dir);
  }
}
