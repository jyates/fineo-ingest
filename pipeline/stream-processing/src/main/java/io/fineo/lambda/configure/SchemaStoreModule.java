package io.fineo.lambda.configure;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static io.fineo.etl.FineoProperties.DYNAMO_SCHEMA_STORE_TABLE;
import static io.fineo.etl.FineoProperties.DYNAMO_WRITE_LIMIT;

public class SchemaStoreModule extends AbstractModule implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaStoreModule.class);

  @Override
  protected void configure() {
  }

  @Provides
  @Inject
  @Singleton
  public SchemaStore getSchemaStore(CreateTableRequest create, AmazonDynamoDBAsyncClient dynamo) {
    LOG.debug("Creating schema repository");
    DynamoDBRepository repo =
      new DynamoDBRepository(new ValidatorFactory.Builder().build(), dynamo, create);
    LOG.debug("created schema repository");
    return new SchemaStore(repo);
  }

  @Provides
  @Inject
  public CreateTableRequest getDynamoSchemaTable(
    @Named(DYNAMO_SCHEMA_STORE_TABLE) String storeTable,
    @Named(DYNAMO_WRITE_LIMIT) Long read, @Named(DYNAMO_WRITE_LIMIT) Long write) {
    CreateTableRequest create =
      DynamoDBRepository.getBaseTableCreate(storeTable);
    create.setProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(read)
      .withWriteCapacityUnits(write));
    return create;
  }
}
