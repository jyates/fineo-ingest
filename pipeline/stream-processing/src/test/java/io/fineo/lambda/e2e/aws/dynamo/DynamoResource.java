package io.fineo.lambda.e2e.aws.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.etl.FineoProperties;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.TableUtils;
import io.fineo.lambda.dynamo.avro.AvroDynamoReader;
import io.fineo.lambda.e2e.aws.AwsResource;
import io.fineo.lambda.e2e.manager.collector.OutputCollector;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.fineo.etl.FineoProperties.DYNAMO_SCHEMA_STORE_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamoResource implements AwsResource {
  private static final Log LOG = LogFactory.getLog(DynamoResource.class);

  public static final String FINEO_DYNAMO_RESOURCE_CLEANUP = "fineo.dynamo.resource.cleanup";

  private final LambdaClientProperties props;
  private final AtomicReference<SchemaStore> storeRef = new AtomicReference<>();
  private final AmazonDynamoDBAsyncClient dynamo;
  private final ResultWaiter.ResultWaiterFactory waiter;
  private final Provider<SchemaStore> store;
  private final String prefix;
  private final String storeTable;
  private final DynamoTableTimeManager manager;
  private boolean cleanup = true;

  @Inject
  public DynamoResource(AmazonDynamoDBAsyncClient dynamo, ResultWaiter.ResultWaiterFactory waiter,
    Provider<SchemaStore> store, LambdaClientProperties props,
    DynamoTableTimeManager manager,
    @Named(FineoProperties.DYNAMO_INGEST_TABLE_PREFIX) String prefix,
    @Named(DYNAMO_SCHEMA_STORE_TABLE) String storeTable) {
    this.props = props;
    this.dynamo = dynamo;
    this.store = store;
    this.waiter = waiter;
    this.prefix = prefix;
    this.storeTable = storeTable;
    this.manager = manager;
  }

  @Inject(optional = true)
  public void setCleanupTables(@Named(FINEO_DYNAMO_RESOURCE_CLEANUP) boolean cleanup) {
    this.cleanup = cleanup;
  }

  public void setup(FutureWaiter future) {
    future.run(() -> {
      storeRef.set(store.get());
      LOG.debug("Schema store creation complete!");
    });
  }

  public SchemaStore getStore() {
    return storeRef.get();
  }

  private void deleteDynamoTables(String nonInclusiveTableNamePrefix) {
    LOG.debug("Starting to delete dynamo table with non-inclusive prefix: " +
              nonInclusiveTableNamePrefix);
    getTables(nonInclusiveTableNamePrefix).parallel().forEach(name -> {
      dynamo.deleteTable(name);
      waiter.get()
            .withDescription("Deleting dynamo table: " + name)
            .withStatusNull(() -> dynamo.describeTable(name))
            .waitForResult();
    });
  }

  public void cleanup(FutureWaiter futures) {
    if (!cleanup) {
      return;
    }
    futures.run(this::deleteSchemaStore);
    futures.run(this::cleanupStoreTables);
  }

  private void deleteSchemaStore() {
    // need less that than the full name since is an exclusive start key and wont include the
    // table we actually want to delete
    this.deleteDynamoTables(storeTable.substring(0, storeTable.length() - 2));
  }

  private void cleanupStoreTables() {
    deleteDynamoTables(prefix);
  }

  public void copyStoreTables(OutputCollector output) {
    getTables(prefix).parallel().forEach(name -> {
      try {
        // create a directory for each table;
        DynamoDB db = new DynamoDB(dynamo);
        Table table = db.getTable(name);
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(output.get(name)));
        for (Item result : table.scan()) {
          for (Map.Entry<String, Object> pair : result.asMap().entrySet()) {
            LOG.trace("Copied: " + pair.getKey() + "=> " + pair.getValue());
            dos.writeBytes(pair.getKey() + " => " + pair.getValue() + "\n");
          }
        }
        dos.flush();
        dos.close();
      } catch (Exception e) {
        LOG.error("Failed to write dynamo data for table: " + name, e);
        throw new RuntimeException(e);
      }
    });
  }

  private Stream<String> getTables(String prefix) {
    String testPrefix = props.getTestPrefix();
    if (testPrefix != null) {
      Preconditions.checkArgument(prefix.startsWith(testPrefix),
        "Table names have to start with the test prefix: %s. Got prefix: %s", testPrefix, prefix);
    }
    Iterable<String> names = () -> TableUtils.getTables(dynamo, prefix, 1);
    return StreamSupport.stream(names.spliterator(), true);
  }

  public List<GenericRecord> read(RecordMetadata metadata) {
    ListTablesResult tablesResult = dynamo.listTables(prefix);
    assertEquals("Wrong number of tables after prefix '" + prefix + "'. Got tables: " + tablesResult
      .getTableNames(), 2, tablesResult.getTableNames().size());

    AvroDynamoReader reader = new AvroDynamoReader(getStore(), dynamo, manager);
    Metric metric = getStore().getMetricMetadata(metadata);
    long ts = metadata.getBaseFields().getTimestamp();
    Range<Instant> range = Range.of(ts, ts + 1);
    ResultWaiter<List<GenericRecord>> waiter =
      this.waiter.get()
                 .withDescription("Avro records to appear in output tables")
                 .withStatus(() ->
                   reader.scan(metadata.getOrgID(), metric, range)
                         .collect(Collectors.toList()))
                 .withStatusCheck(list -> ((List<GenericRecord>) list).size() > 0);
    assertTrue("Didn't get any rows from Dynamo within timeout!", waiter.waitForResult());
    return waiter.getLastStatus();
  }
}
