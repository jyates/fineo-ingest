package io.fineo.lambda.configure.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;

public class DynamoModule extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  @Inject
  @Singleton
  public AvroToDynamoWriter getDynamoWriter(AmazonDynamoDBAsyncClient client,
    @Named(LambdaClientProperties.DYNAMO_INGEST_TABLE_PREFIX) String prefix,
    @Named(LambdaClientProperties.DYNAMO_READ_LIMIT) long readLimit,
    @Named(LambdaClientProperties.DYNAMO_WRITE_LIMIT) long writeLimit,
    @Named(LambdaClientProperties.DYNAMO_RETRIES) long retries) {
    return new AvroToDynamoWriter(client, prefix, readLimit, writeLimit, retries);
  }
}
