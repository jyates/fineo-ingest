package io.fineo.lambda;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.LogType;
import com.fasterxml.jackson.jr.ob.JSON;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.avro.LambdaClientProperties;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

/**
 * Similar to the local TestEndToEndLambda, but leverages actual AWS services to support
 * access
 */
@Category(AwsDependentTests.class)
public class TestEndToEndLambdaAws {

  private static final Log LOG = LogFactory.getLog(TestEndToEndLambdaAws.class);

  @ClassRule
  public static AwsCredentialResource awsCredentials = new AwsCredentialResource();
  private final String region = System.getProperty("aws-region", "us-east-1");

  private static final String START_ARN =
    "arn:aws:lambda:us-east-1:766732214526:function:RawToAvro";

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

  @Test
  public void testConnect() throws Exception {
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    setupSchema(json);

    makeRequest(json);

    validate();
  }

  private void setupSchema(Map<String, Object> json) throws Exception {
    LambdaClientProperties props = LambdaClientProperties.load();
    props.setAwsCredentialProviderForTesting(awsCredentials.getProvider());
    LOG.info("Start");
    SchemaStore store = props.createSchemaStore();
    LambdaTestUtils.updateSchemaStore(store, json);
  }

  private void makeRequest(Map<String, Object> json) throws IOException, OldSchemaException {
    ByteBuffer bytes = LambdaTestUtils.asBytes(json);
    String data = Base64.getEncoder().encodeToString(Arrays.copyOfRange(bytes.array(), bytes
      .arrayOffset(), bytes.remaining()));
    LOG.info("Data: " + data);

    InvokeRequest request = new InvokeRequest();
    request.setFunctionName(START_ARN);
    request.withPayload(String.format(TEST_EVENT, data, region));
    request.setInvocationType(InvocationType.RequestResponse);
    request.setLogType(LogType.Tail);

    AWSLambdaClient client = new AWSLambdaClient(awsCredentials.getProvider());
    InvokeResult result = client.invoke(request);
    LOG.info("Status: " + result.getStatusCode());
    LOG.info("Error:" + result.getFunctionError());
    LOG.info("Log:" + new String(Base64.getDecoder().decode(result.getLogResult())));
  }

  private void validate() {

  }
}