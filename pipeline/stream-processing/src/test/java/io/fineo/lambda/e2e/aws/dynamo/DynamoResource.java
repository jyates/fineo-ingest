package io.fineo.lambda.e2e.aws.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.etl.FineoProperties;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.ResultOrException;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.lambda.dynamo.TableUtils;
import io.fineo.lambda.dynamo.avro.AvroDynamoReader;
import io.fineo.lambda.dynamo.iter.PageManager;
import io.fineo.lambda.dynamo.iter.PagingIterator;
import io.fineo.lambda.dynamo.iter.ScanPager;
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
  public void setCleanupTables(@Named(FINEO_DYNAMO_RESOURCE_CLEANUP) boolean cleanup){
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
    if(!cleanup){
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
        ScanRequest request = new ScanRequest(name);
        ScanPager runner = new ScanPager(dynamo, request, Schema.PARTITION_KEY_NAME, null);
        Iterable<ResultOrException<Map<String, AttributeValue>>> iter =
          () -> new PagingIterator<>(5, new PageManager(Lists.newArrayList(runner)));
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(output.get(name)));
        for (ResultOrException<Map<String, AttributeValue>> result : iter) {
          result.doThrow();
          Map<String, AttributeValue> row = result.getResult();
          for (Map.Entry<String, AttributeValue> pair : row.entrySet()) {
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
    return TableUtils.getTables(dynamo, prefix, 1);
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
                 .withDescription("Metadata records to appear in schema store: " + storeTable)
                 .withStatus(() ->
                   reader.scan(metadata.getOrgID(), metric, range, null)
                         .collect(Collectors.toList()))
                 .withStatusCheck(list -> ((List<GenericRecord>) list).size() > 0);
    assertTrue("Didn't get any rows from Dynamo within timeout!", waiter.waitForResult());
    return waiter.getLastStatus();
  }
}
