package io.fineo.lambda.configure;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.schemarepo.ValidatorFactory;

import java.util.Properties;

import static io.fineo.lambda.configure.LambdaClientProperties.*;

public class LambdaModule extends AbstractModule {
  private static final Log LOG = LogFactory.getLog(LambdaModule.class);
  private final Properties props;
  private final Provider<AWSCredentialsProvider> creds;

  public LambdaModule(Properties props, Provider<AWSCredentialsProvider> credentials) {
    this.props = props;
    this.creds = credentials;
  }

  @Override
  protected void configure() {
    // legacy binding for reading properties directly
    bind(Properties.class).toInstance(props);
    Names.bindProperties(this.binder(), props);
    bind(AWSCredentialsProvider.class).toProvider(creds);
  }

  @Provides
  @Inject
  public AmazonDynamoDBAsyncClient getDynamoClient(AWSCredentialsProvider provider,
    AwsDynamoConfigurator configurator) {
    LOG.debug("Creating dynamo with provider: " + provider);
    AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient(provider);
    configurator.configure(client);
    LOG.debug("Got client, setting endpoint");
    return client;
  }

  @Provides
  @Inject
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
