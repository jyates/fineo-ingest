package io.fineo.etl.lambda.chore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.handle.LambdaWrapper;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Wrapper around {@link CleanOldDynamoTablesImpl}
 */
public class CleanOldDynamoTables extends LambdaWrapper<Object, CleanOldDynamoTablesImpl> {

  public CleanOldDynamoTables() throws IOException {
    this(getModules());
  }

  public CleanOldDynamoTables(List<Module> modules) {
    super(CleanOldDynamoTablesImpl.class, modules);
  }

  @Override
  public void handle(Object event) throws IOException {
    getInstance().handle(event);
  }

  public static List<Module> getModules() throws IOException {
    return newArrayList(
      new DefaultCredentialsModule(),
      PropertiesModule.load(),
      new DynamoModule(),
      new DynamoRegionConfigurator(),
      new AbstractModule() {
        @Override
        protected void configure() {
        }

        @Provides
        @Inject
        public DynamoDB getDynamo(AmazonDynamoDBAsyncClient client) {
          return new DynamoDB(client);
        }
      }
    );
  }
}
