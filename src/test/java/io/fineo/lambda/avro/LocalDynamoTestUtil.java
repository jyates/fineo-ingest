package io.fineo.lambda.avro;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import io.fineo.aws.rule.AwsCredentialResource;

import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Utility instance to help create and run a local dynamo instance
 */
public class LocalDynamoTestUtil {

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

  public AmazonDynamoDBClient start() throws Exception {
    // create a local database instance with an local server url on an open port
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    final String[] localArgs = {"-inMemory", "-port", String.valueOf(port)};
    server = ServerRunner.createServerFromCommandLineArgs(localArgs);
    server.start();
    url = "http://localhost:" + port;

    dynamodb = new AmazonDynamoDBClient(credentials.getFakeProvider());
    dynamodb.setEndpoint("http://localhost:" + port);
    return dynamodb;
  }

  public void stop() throws Exception {
    server.stop();
    dynamodb.shutdown();
  }

  public void setConnectionProperties(Properties props) {
    props.setProperty(FirehoseClientProperties.DYNAMO_ENDPOINT, url);
    props.setProperty(FirehoseClientProperties.DYNAMO_SCHEMA_STORE_TABLE, storeTableName);
    props.setProperty(FirehoseClientProperties.DYNAMO_INGEST_TABLE_PREFIX, ingestPrefix);
    props.setProperty(FirehoseClientProperties.DYNAMO_READ_LIMIT, "10");
    props.setProperty(FirehoseClientProperties.DYNAMO_WRITE_LIMIT, "10");
  }

  public void cleanupTables(boolean storeTableCreated) {
    // cleanup anything with the ingest prefix. Ingest prefix is assumed to start after any other
    // table names, for the sake of this test utility, so we just get the last group of tables
    ListTablesRequest list = new ListTablesRequest(ingestPrefix, 50);
    dynamodb.listTables(list)
            .getTableNames()
            .forEach(name -> dynamodb.deleteTable(name));

    if (storeTableCreated) {
      dynamodb.deleteTable(storeTableName);
    } else {
      assertEquals("Created tables when didn't use store", 0,
        dynamodb.listTables().getTableNames().size());
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
}
