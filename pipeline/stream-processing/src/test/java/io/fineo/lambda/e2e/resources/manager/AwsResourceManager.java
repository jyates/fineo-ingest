package io.fineo.lambda.e2e.resources.manager;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.EndtoEndSuccessStatus;
import io.fineo.lambda.e2e.resources.AwsResource;
import io.fineo.lambda.e2e.resources.ResourceUtils;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.dynamo.DynamoResource;
import io.fineo.lambda.e2e.resources.firehose.FirehoseResource;
import io.fineo.lambda.e2e.resources.kinesis.KinesisStreamManager;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
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

  private final AwsCredentialResource awsCredentials;
  private final TestOutput output;

  private LambdaClientProperties props;
  private FirehoseResource firehose;
  private KinesisStreamManager kinesis;
  private DynamoResource dynamo;
  private List<AwsResource> resources = new ArrayList<>();
  private boolean cleanup;

  public AwsResourceManager(AwsCredentialResource awsCredentials, TestOutput output,
    LambdaKinesisConnector connector, String region) {
    super(connector);
    this.awsCredentials = awsCredentials;
    this.output = output;
    this.region = region;
  }

  /**
   * Cleanup the AWS resources in the case of failure
   */
  public void cleanupResourcesOnFailure(boolean cleanup) {
    this.cleanup = cleanup;
  }

  @Override
  public void setup(LambdaClientProperties props) throws Exception {
    this.props = props;
    FutureWaiter future = new FutureWaiter(executor);
    ResultWaiter.ResultWaiterFactory waiter = new ResultWaiter.ResultWaiterFactory(TestProperties
      .FIVE_MINUTES, TestProperties.ONE_SECOND);

    this.dynamo = new DynamoResource(props, waiter);
    dynamo.setup(future);
    resources.add(dynamo);

    // setup the firehose connections
    this.firehose = new FirehoseResource(props, awsCredentials.getProvider(), waiter);
    for (String stage : TestProperties.Lambda.STAGES) {
      firehose.createFirehoses(stage, future);
    }
    resources.add(firehose);

    // setup the kinesis streams + interconnection
    this.kinesis = new KinesisStreamManager(awsCredentials.getProvider(), waiter, region,
      TestProperties.Kinesis.SHARD_COUNT);
    future.run(() -> {
      try {
        connector.connect(props, this.kinesis);
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
    assertEquals(Lists.newArrayList(records.get(0)), records);
    EndToEndTestRunner.verifyRecordMatchesJson(getStore(), json, records.get(0));
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
          cloneS3(LambdaClientProperties.RAW_PREFIX);
          cloneS3(LambdaClientProperties.STAGED_PREFIX);

          // copy kinesis data
          File out = output.newFolder("kinesis");
          for (String streamName : kinesis.getStreamNames()) {
            ResourceUtils.writeStream(streamName, out, () -> this.connector.getWrites(streamName));
          }
          // copy any dynamo data
          dynamo.copyStoreTables(output.newFolder(LambdaClientProperties.STAGED_PREFIX, "dynamo"));
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
    List<Pair<String, LambdaClientProperties.StreamType>> toClone = new ArrayList<>(3);
    for (LambdaClientProperties.StreamType t : LambdaClientProperties.StreamType.values()) {
      LOG.debug(format("Cloning s3 [%s => %s]", stage, t));
      toClone.add(new Pair<>(stage, t));
    }
    File dir = output.newFolder(stage, "s3");
    firehose.clone(toClone, dir);
  }
}