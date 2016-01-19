package io.fineo.lambda.avro;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import io.fineo.aws.rule.AwsCredentialResource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Utility instance to help create and run a local dynamo instance
 */
public class LocalDynamoTestUtil {

  private static final Log LOG = LogFactory.getLog(LocalDynamoTestUtil.class);
  private AmazonDynamoDBClient dynamodb;
  private DynamoDBProxyServer server;

  private int port;
  private String url;
  private final AwsCredentialResource credentials;
  private String storeTableName = generateTableName();
  private Random random = new Random();
  private String ingestPrefix = generateIngestPrefix();

  public LocalDynamoTestUtil(AwsCredentialResource credentials) {
    this.credentials = credentials;
  }

  public void start() throws Exception {
    // create a local database instance with an local server url on an open port
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    final String[] localArgs = {"-inMemory", "-port", String.valueOf(port)};
    server = ServerRunner.createServerFromCommandLineArgs(localArgs);
    server.start();
    url = "http://localhost:" + port;

    // internal client connection so we can easily stop, cleanup, etc. later
    this.dynamodb = getClient();
  }

  public void stop() throws Exception {
    server.stop();
    dynamodb.shutdown();
  }

  public void setConnectionProperties(Properties props) {
    props.setProperty(LambdaClientProperties.DYNAMO_URL_FOR_TESTING, url);
    props.setProperty(LambdaClientProperties.DYNAMO_SCHEMA_STORE_TABLE, storeTableName);
    props.setProperty(LambdaClientProperties.DYNAMO_INGEST_TABLE_PREFIX, ingestPrefix);
    props.setProperty(LambdaClientProperties.DYNAMO_READ_LIMIT, "10");
    props.setProperty(LambdaClientProperties.DYNAMO_WRITE_LIMIT, "10");
    props.setProperty(LambdaClientProperties.DYNAMO_RETRIES, "1");
  }

  public void cleanupTables(boolean storeTableCreated) {
    // cleanup anything with the ingest prefix. Ingest prefix is assumed to start after any other
    // table names, for the sake of this test utility, so we just get the last group of tables
    ListTablesRequest list = new ListTablesRequest(ingestPrefix, 50);
    dynamodb.listTables(list)
            .getTableNames()
            .parallelStream().peek(name -> LOG.info("Deleting table: " + name))
            .forEach(name -> dynamodb.deleteTable(name));

    if (storeTableCreated) {
      dynamodb.deleteTable(storeTableName);
    } else {
      List<String> tables = dynamodb.listTables().getTableNames();
      assertEquals("Created tables when didn't use store", new ArrayList<>(), tables);
    }

    // get the next table name
    this.storeTableName = generateTableName();
    this.ingestPrefix = generateIngestPrefix();
  }

  private String generateTableName() {
    return "kinesis-avro-test-" + UUID.randomUUID().toString();
  }

  private String generateIngestPrefix() {
    return "z-test-ingest-" + random.nextInt(500);
  }

  public String getCurrentTestTable() {
    return this.storeTableName;
  }

  public AmazonDynamoDBClient getClient() {
    return withProvider(new AmazonDynamoDBClient(credentials.getFakeProvider()));
  }

  public AmazonDynamoDBAsyncClient getAsyncClient() {
    return withProvider(new AmazonDynamoDBAsyncClient(credentials.getFakeProvider()));
  }

  private <T extends AmazonWebServiceClient> T withProvider(T client) {
    client.setEndpoint("http://localhost:" + port);
    return client;
  }
}
