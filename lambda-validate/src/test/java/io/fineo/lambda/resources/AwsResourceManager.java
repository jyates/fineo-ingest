package io.fineo.lambda.resources;

import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.LogType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.EndtoEndSuccessStatus;
import io.fineo.lambda.e2e.TestOutput;
import io.fineo.lambda.e2e.resources.DynamoResource;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.firehose.FirehoseResource;
import io.fineo.lambda.e2e.resources.kinesis.KinesisStreamManager;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.lambda.util.run.FutureWaiter;
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
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static io.fineo.lambda.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.LambdaClientProperties.StreamType.ARCHIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Manages lambda test resources on AWS
 */
public class AwsResourceManager implements ResourceManager {
  private static final Log LOG = LogFactory.getLog(AwsResourceManager.class);
  private static final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
    MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool()));

  private static final String TEST_EVENT =
    "{"
    + "  \"Records\": ["
    + "    {"
    + "      \"eventID\": "
    + "\"shardId-000000000000:49545115243490985018280067714973144582180062593244200961\","
    + "      \"eventVersion\": \"1.0\","
    + "      \"kinesis\": {"
    + "        \"partitionKey\": \"partitionKey-3\","
    + "        \"data\": \"%s\","
    + "        \"kinesisSchemaVersion\": \"1.0\","
    + "        \"sequenceNumber\": \"49545115243490985018280067714973144582180062593244200961\""
    + "      },"
    + "      \"invokeIdentityArn\": \"arn:aws:iam::EXAMPLE\","
    + "      \"eventName\": \"aws:kinesis:record\","
    + "      \"eventSourceARN\": \"arn:aws:kinesis:EXAMPLE\","
    + "      \"eventSource\": \"aws:kinesis\","
    + "      \"awsRegion\": \"%s\""
    + "    }"
    + "  ]"
    + "}"
    + "";

  private final String region = System.getProperty("aws-region", "us-east-1");
  private final String RAW_TO_AVRO_ARN = TestProperties.Lambda.getRawToAvroArn(region);

  private final AwsCredentialResource awsCredentials;
  private final TestOutput output;
  private LambdaClientProperties props;
  private FirehoseResource firehose;
  private KinesisLambdaManager kinesis;
  private DynamoResource dynamo;

  public AwsResourceManager(AwsCredentialResource awsCredentials, TestOutput output) {
    this.awsCredentials = awsCredentials;
    this.output = output;
  }

  @Override
  public void setup(LambdaClientProperties props) throws Exception {
    this.props = props;
    FutureWaiter future = new FutureWaiter(executor);
    this.dynamo = new DynamoResource(props);
    dynamo.setup(future);

    // setup the firehose connections
    this.firehose = new FirehoseResource(props, awsCredentials.getProvider());
    for (String stage : TestProperties.Lambda.STAGES) {
      firehose.createFirehoses(stage, future);
    }

    // setup the kinesis streams
    this.kinesis = new KinesisLambdaManager(props, awsCredentials.getProvider(), future);
    future.run(() -> {
      kinesis.setup(region);
    });

    // wait for all the setup to complete
    future.await();
  }


  @Override
  public byte[] send(Map<String, Object> json) throws Exception {
    LOG.info("------ > Making request ----->");
    byte[] array = LambdaTestUtils.asBytes(json);
    String data = Base64.getEncoder().encodeToString(array);
    LOG.info("With data: " + data);

    InvokeRequest request = new InvokeRequest();
    request.setFunctionName(RAW_TO_AVRO_ARN);
    request.withPayload(String.format(TEST_EVENT, data, region));
    request.setInvocationType(InvocationType.RequestResponse);
    request.setLogType(LogType.Tail);

    AWSLambdaClient client = new AWSLambdaClient(awsCredentials.getProvider());
    InvokeResult result = client.invoke(request);
    LOG.info("Status: " + result.getStatusCode());
    LOG.info("Error:" + result.getFunctionError());
    LOG.info("Log:" + new String(Base64.getDecoder().decode(result.getLogResult())));
    assertNull(result.getFunctionError());
    return array;
  }

  @Override
  public List<ByteBuffer> getFirhoseWrites(String stream) {
    return firehose.read(stream);
  }

  @Override
  public List<ByteBuffer> getKinesisWrites(String stream) {
    return kinesis.getWrites(stream);
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
    FutureWaiter futures = new FutureWaiter(executor);
    futures.run(dynamo::deleteSchemaStore);
    if (status != null) {
      if (!status.isSuccessful()) {
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

      // cleanup all the resources
      kinesis.deleteStreams(futures);
      firehose.cleanupFirehoses(futures);
      futures.run(this.firehose::cleanupData);
      futures.run(dynamo::cleanupStoreTables);
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
      kinesis.clone(props.getRawToStagedKinesisStreamName(), output.newFolder
        (LambdaClientProperties.STAGED_PREFIX, "kinesis"));
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
