package io.fineo.lambda.configure;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.schemarepo.ValidatorFactory;

import java.util.Properties;

import static io.fineo.lambda.configure.LambdaClientProperties.DYNAMO_SCHEMA_STORE_TABLE;
import static io.fineo.lambda.configure.LambdaClientProperties.DYNAMO_WRITE_LIMIT;

public class LambdaModule extends AbstractModule {
  private static final Log LOG = LogFactory.getLog(LambdaModule.class);
  private final Properties props;

  public LambdaModule(Properties props) {
    this.props = props;
  }

  @Override
  protected void configure() {
    // legacy binding for reading properties directly
    bind(Properties.class).toInstance(props);
    Names.bindProperties(this.binder(), props);
  }

  @Provides
  @Inject
  @Singleton
  public SchemaStore getSchemaStore(CreateTableRequest create,
    Provider<AmazonDynamoDBAsyncClient> dynamo) {
    LOG.debug("Creating schema repository");
    DynamoDBRepository repo =
      new DynamoDBRepository(new ValidatorFactory.Builder().build(), dynamo.get(), create);
    LOG.debug("created schema repository");
    return new SchemaStore(repo);
  }

  @Provides
  @Inject
  @Singleton
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
