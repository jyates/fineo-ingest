package io.fineo.lambda.avro;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import io.fineo.aws.rule.AwsCredentialResource;

import java.net.ServerSocket;

/**
 * Utility instance to help create and run a local dynamo instance
 */
public class LocalDynamoTestUtil {

  private AmazonDynamoDBClient dynamodb;
  private DynamoDBProxyServer server;

  private int port;
  private String url;
  private final AwsCredentialResource credentials;

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
}
