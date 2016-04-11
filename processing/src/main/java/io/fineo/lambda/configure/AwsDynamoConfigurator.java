package io.fineo.lambda.configure;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;

public interface AwsDynamoConfigurator {
  void configure(AmazonDynamoDBAsyncClient client);
}
