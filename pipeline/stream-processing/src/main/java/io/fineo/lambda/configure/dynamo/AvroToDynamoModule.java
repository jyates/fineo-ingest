package io.fineo.lambda.configure.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;

import java.io.Serializable;

public class AvroToDynamoModule extends AbstractModule implements Serializable {

  @Provides
  @Inject
  @Singleton
  public AvroToDynamoWriter getDynamoWriter(AmazonDynamoDBAsyncClient client,
    @Named(FineoProperties.DYNAMO_INGEST_TABLE_PREFIX) String prefix,
    @Named(FineoProperties.DYNAMO_READ_LIMIT) long readLimit,
    @Named(FineoProperties.DYNAMO_WRITE_LIMIT) long writeLimit,
    @Named(FineoProperties.DYNAMO_RETRIES) long retries) {
    return new AvroToDynamoWriter(client, prefix, readLimit, writeLimit, retries);
  }

  @Override
  protected void configure() {
  }
}
