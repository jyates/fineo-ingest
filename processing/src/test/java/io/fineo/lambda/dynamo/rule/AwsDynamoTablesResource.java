package io.fineo.lambda.dynamo.rule;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.collect.Lists;
import io.fineo.lambda.dynamo.LocalDynamoTestUtil;
import io.fineo.lambda.dynamo.iter.PageManager;
import io.fineo.lambda.dynamo.iter.PagingIterator;
import io.fineo.lambda.dynamo.iter.TableNamePager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.rules.ExternalResource;

import java.util.Iterator;

/**
 * Manage aws tables and getting a connection to them. Generally, this should be used at the
 * {@link org.junit.Rule} level.
 */
public class AwsDynamoTablesResource extends ExternalResource {

  private static final Log LOG = LogFactory.getLog(AwsDynamoTablesResource.class);

  private final AwsDynamoResource dynamoResource;
  private LocalDynamoTestUtil util;
  private AmazonDynamoDBAsyncClient client;

  public AwsDynamoTablesResource(AwsDynamoResource dynamo) {
    this.dynamoResource = dynamo;
  }

  @Override
  protected void after() {
    try {
      // cleanup anything with the ingest prefix. Ingest prefix is assumed to start after any other
      // table names, for the sake of this test utility, so we just get the last group of tables
      for (String name : new PagingIterator<>(50,
        new PageManager<>(Lists.newArrayList(new TableNamePager("", getAsyncClient(), 50))))
        .iterable()) {
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
}