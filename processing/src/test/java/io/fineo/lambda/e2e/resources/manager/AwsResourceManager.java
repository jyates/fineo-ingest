package io.fineo.lambda.e2e.resources.manager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.EndtoEndSuccessStatus;
import io.fineo.lambda.e2e.TestOutput;
import io.fineo.lambda.e2e.resources.AwsResource;
import io.fineo.lambda.e2e.resources.DynamoResource;
import io.fineo.lambda.e2e.resources.ResourceUtils;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.firehose.FirehoseResource;
import io.fineo.lambda.e2e.resources.kinesis.KinesisStreamManager;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
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

import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.StreamType.ARCHIVE;
import static org.junit.Assert.assertEquals;

/**
 * Manages lambda test resources on AWS
 */
public class AwsResourceManager extends BaseResourceManager {
  private static final Log LOG = LogFactory.getLog(AwsResourceManager.class);
  private static final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
    MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));

  private final String region = System.getProperty("aws-region", "us-east-1");

  private final AwsCredentialResource awsCredentials;
  private final TestOutput output;

  private LambdaClientProperties props;
  private FirehoseResource firehose;
  private KinesisStreamManager kinesis;
  private DynamoResource dynamo;
  private List<AwsResource> resources = new ArrayList<>();

  public AwsResourceManager(AwsCredentialResource awsCredentials, TestOutput output,
    LambdaKinesisConnector connector) {
    super(connector);
    this.awsCredentials = awsCredentials;
    this.output = output;
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
    if (status != null && !status.isSuccessful()) {
      LOG.info(" ---- FAILURE ----");
      LOG.info("");
      LOG.info("Data available at: " + output.getRoot());
      LOG.info("");
      LOG.info(" ---- FAILURE ----");
      // we didn't start the test properly, so ensure that no data was stored in firehoses
      if (!status.isMessageSent() || !status.isUpdateStoreCorrect()) {
        firehose.ensureNoDataStored();
      } else {
        // if is not so good, but we don't want to be keep resources up, so cleanup after we pull
        // down everything that is in error
        Preconditions.checkState(status.isAvroToStorageSuccessful(),
          "Last lambda stage was successful, but not overall successful");
        if (!status.isRawToAvroSuccessful()) {
          cloneRawToAvroData(status);
        } else {
          cloneAvroToStorageData(status);
        }
      }
    }
    deleteResources();
  }

  private void deleteResources() throws InterruptedException {
    FutureWaiter futures = new FutureWaiter(executor);
    for (AwsResource resource : resources) {
      resource.cleanup(futures);
    }

    futures.await();
  }

  /**
   * Copy all the resources that were part of the failure to local disk fpor the raw -> avro stage
   */
  private void cloneRawToAvroData(EndtoEndSuccessStatus status) throws IOException {
    cloneS3(LambdaClientProperties.RAW_PREFIX, status);
  }

  /**
   * Copy all the resources that were part of the failure to local disk fpor the raw -> avro stage
   */
  private void cloneAvroToStorageData(EndtoEndSuccessStatus status) throws IOException {
    cloneS3(LambdaClientProperties.STAGED_PREFIX, status);
    // we didn't even archive anything, so clone down the kinesis contents
    if (!status.getCorrectFirehoses().contains(new Pair<>(STAGED_PREFIX, ARCHIVE))) {
      String streamName = props.getRawToStagedKinesisStreamName();
      File out = output.newFolder(LambdaClientProperties.STAGED_PREFIX, "kinesis");
      ResourceUtils.writeStream(streamName, out, () -> this.connector.getWrites(streamName));
    }

    // copy any data from Dynamo
    dynamo.copyStoreTables(output.newFolder(LambdaClientProperties.STAGED_PREFIX, "dynamo"));
  }

  private void cloneS3(String stage, EndtoEndSuccessStatus status) throws IOException {
    List<Pair<String, LambdaClientProperties.StreamType>> toClone = new ArrayList<>(3);
    LOG.debug("Raw -> Avro successful - cleaning up all endpoints");
    for (LambdaClientProperties.StreamType t : LambdaClientProperties.StreamType.values()) {
      String firehose = props.getFirehoseStreamName(stage, t);
      if (status.getCorrectFirehoses().contains(firehose)) {
        toClone.add(new Pair<>(stage, t));
      }
    }
    File dir = output.newFolder(stage, "s3");
    firehose.clone(toClone, dir);
  }
}
