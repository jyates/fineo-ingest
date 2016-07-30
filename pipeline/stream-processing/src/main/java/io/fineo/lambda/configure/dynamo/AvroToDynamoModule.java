package io.fineo.lambda.configure.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;

import java.io.Serializable;

public class AvroToDynamoModule extends AbstractModule implements Serializable {

  @Provides
  @Inject
  @Singleton
  public DynamoTableTimeManager getDynamoTableManager(AmazonDynamoDBAsyncClient client,
    @Named(FineoProperties.DYNAMO_INGEST_TABLE_PREFIX) String prefix) {
    return new DynamoTableTimeManager(client, prefix);
  }

  @Provides
  @Inject
  @Singleton
  public DynamoTableCreator getDynamoTableCreator(DynamoTableTimeManager tables,
    DynamoDB dynamo,
    @Named(FineoProperties.DYNAMO_READ_LIMIT) long readLimit,
    @Named(FineoProperties.DYNAMO_WRITE_LIMIT) long writeLimit) {
    return new DynamoTableCreator(tables, dynamo, readLimit, writeLimit);
  }

  @Provides
  @Inject
  @Singleton
  public AvroToDynamoWriter getDynamoWriter(AmazonDynamoDBAsyncClient client,
    @Named(FineoProperties.DYNAMO_RETRIES) long retries,
    DynamoTableCreator tables) {
    return new AvroToDynamoWriter(client, retries, tables);
  }

  @Override
  protected void configure() {
  }
}
