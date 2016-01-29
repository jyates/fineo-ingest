package io.fineo.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.LogType;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.avro.AvroDynamoReader;
import io.fineo.lambda.resources.FirehoseManager;
import io.fineo.lambda.resources.KinesisManager;
import io.fineo.lambda.util.EndToEndTestRunner;
import io.fineo.lambda.util.EndtoEndSuccessStatus;
import io.fineo.lambda.util.FutureWaiter;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.lambda.util.ResultWaiter;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
  private LambdaClientProperties props;
  private SchemaStore store;
  private FirehoseManager firehose;
  private KinesisManager kinesis;

  public AwsResourceManager(AwsCredentialResource awsCredentials) {
    this.awsCredentials = awsCredentials;
  }

  @Override
  public void setup(LambdaClientProperties props) throws Exception {
    this.props = props;
    SchemaStore[] storeRef = new SchemaStore[1];
    FutureWaiter future = new FutureWaiter(executor);
    future.run(() -> {
      SchemaStore store = props.createSchemaStore();
      storeRef[0] = store;
      LOG.debug("Schema store creation complete!");
    });

    // setup the firehose connections
    this.firehose = new FirehoseManager(props, awsCredentials.getProvider());
    for (String stage : TestProperties.Lambda.STAGES) {
      firehose.createFirehoses(stage, future);
    }

    // setup the kinesis streams
    this.kinesis = new KinesisManager(props, awsCredentials.getProvider());
    future.run(() -> {
      kinesis.setup(region);
    });

    // wait for all the setup to complete
    future.await();
    this.store = storeRef[0];
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
    return this.store;
  }

  @Override
  public void verifyDynamoWrites(RecordMetadata metadata, Map<String, Object> json) {
    String tablePrefix = props.getDynamoIngestTablePrefix();
    AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(awsCredentials.getProvider());
    ListTablesResult tablesResult = client.listTables(tablePrefix);
    assertEquals(
      "Wrong number of tables after prefix" + tablePrefix + ". Got tables: " + tablesResult
        .getTableNames(), 1, tablesResult.getTableNames().size());

    AvroDynamoReader reader = new AvroDynamoReader(getStore(), client, tablePrefix);
    Metric metric = getStore().getMetricMetadata(metadata);
    long ts = metadata.getBaseFields().getTimestamp();
    Range<Instant> range = Range.of(ts, ts + 1);
    ResultWaiter<List<GenericRecord>> waiter = new ResultWaiter<>()
      .withDescription("Some records to appear in dynamo")
      .withStatus(
        () -> reader.scan(metadata.getOrgID(), metric, range, null).collect(Collectors.toList()))
      .withStatusCheck(list -> ((List<GenericRecord>) list).size() > 0);
    assertTrue("Didn't get any rows from Dynamo within timeout!", waiter.waitForResult());
    List<GenericRecord> records = waiter.getLastStatus();
    assertEquals(Lists.newArrayList(records.get(0)), records);
    EndToEndTestRunner.verifyRecordMatchesJson(getStore(), json, records.get(0));
  }

  @Override
  public void cleanup(EndtoEndSuccessStatus status) throws InterruptedException {
    cleanupBasicResources();
    if (status == null) {
      return;
    }

    if (status.isSuccessful()) {
      this.firehose.cleanupData();
      return;
    }

    cleanupNonErrorS3(status);

    cleanupNonErrorDynamo(status);
  }

  private void cleanupNonErrorDynamo(EndtoEndSuccessStatus status) {
    String table = props.getDynamoIngestTablePrefix();
    if (!status.isAvroToStorageSuccessful()) {
      LOG.info("Dynamo table(s) starting with " + table + " was not deleted because there was an "
               + "error validating dynamo");
    }
    deleteDynamoTables(table);
  }

  private void cleanupNonErrorS3(EndtoEndSuccessStatus status) {
    if (!status.isMessageSent() || !status.isUpdateStoreCorrect()) {
      firehose.ensureNoDataStored();
      return;
    }

    // only cleanup the S3 files that we don't need to keep track of because that phase was
    // successful
    List<Pair<String, LambdaClientProperties.StreamType>> toDelete = new ArrayList<>(6);
    LOG.debug("Raw -> Avro successful - cleaning up all endpoints");
    for (String stage : TestProperties.Lambda.STAGES) {
      for (LambdaClientProperties.StreamType t : LambdaClientProperties.StreamType.values()) {
        String firehose = props.getFirehoseStreamName(stage, t);
        if (status.getCorrectFirehoses().contains(firehose)) {
          toDelete.add(new Pair<>(LambdaClientProperties.RAW_PREFIX, t));
        }
      }
    }

    firehose.cleanupData(toDelete);
  }

  private void cleanupBasicResources() throws InterruptedException {
    FutureWaiter futures = new FutureWaiter(executor);
    //dynamo
    futures.run(() -> deleteDynamoTables(props.getSchemaStoreTable()));

    firehose.cleanupFirehoses(futures);

    // kinesis
    kinesis.deleteStreams(futures);
    futures.await();
  }

  private void deleteDynamoTables(String tableNamesPrefix) {
    // dynamo
    ListTablesResult tables = props.getDynamo().listTables(props.getTestPrefix());
    for (String name : tables.getTableNames()) {
      if (!name.startsWith(props.getTestPrefix())) {
        LOG.debug("Stopping deletion of dynamo tables with name: " + name);
        break;
      }
      if (name.startsWith(tableNamesPrefix)) {
        AmazonDynamoDBAsyncClient dynamo = props.getDynamo();
        dynamo.deleteTable(name);
        new ResultWaiter<>()
          .withDescription("Deletion dynamo table: " + name)
          .withStatusNull(() -> dynamo.describeTable(name))
          .waitForResult();
      }
    }
  }
}