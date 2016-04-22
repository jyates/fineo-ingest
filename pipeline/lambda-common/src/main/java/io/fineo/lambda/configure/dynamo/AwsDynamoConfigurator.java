package io.fineo.lambda.configure.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;

public interface AwsDynamoConfigurator {
  void configure(AmazonDynamoDBAsyncClient client);
}
