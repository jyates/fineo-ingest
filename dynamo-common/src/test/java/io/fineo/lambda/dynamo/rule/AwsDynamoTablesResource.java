package io.fineo.lambda.dynamo.rule;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage aws tables and getting a connection to them. Generally, this should be used at the
 * {@link org.junit.Rule} level.
 */
public class AwsDynamoTablesResource extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(AwsDynamoTablesResource.class);

  private final AwsDynamoResource dynamoResource;
  private LocalDynamoTestUtil util;
  private AmazonDynamoDBAsyncClient client;

  public AwsDynamoTablesResource(AwsDynamoResource dynamo) {
    this.dynamoResource = dynamo;
  }

  @Override
  protected void after() {
    try {
      if (getAsyncClient().listTables().getTableNames().size() == 0) {
        return;
      }
      // cleanup anything with the ingest prefix. Ingest prefix is assumed to start after any other
      // table names, for the sake of this test utility, so we just get the last group of tables
      DynamoDB dynamo = new DynamoDB(client);
      for (Table table : dynamo.listTables()) {
        String name = table.getTableName();
        LOG.info("Deleting table: " + name);
        this.getAsyncClient().deleteTable(name);
      }
    } catch (ResourceNotFoundException e) {
      LOG.error("\n----------\n Could not delete a table! ");
      throw e;
    }

    // reset any open clients
    if (client != null) {
      client.shutdown();
      client = null;
    }
  }

  public String getTestTableName() {
    return getUtil().getCurrentTestTable();
  }

  public AmazonDynamoDBAsyncClient getAsyncClient() {
    if (this.client == null) {
      this.client = getUtil().getAsyncClient();
    }
    return this.client;
  }

  private LocalDynamoTestUtil getUtil() {
    if (this.util == null) {
      this.util = dynamoResource.getUtil();
    }
    return this.util;
  }

  public Module getDynamoModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
      }

      @Provides
      @Singleton
      public AmazonDynamoDBAsyncClient getClient() {
        return getAsyncClient();
      }

      @Provides
      @Inject
      public DynamoDB getDB(AmazonDynamoDBAsyncClient client) {
        return new DynamoDB(client);
      }

    };
  }
}
