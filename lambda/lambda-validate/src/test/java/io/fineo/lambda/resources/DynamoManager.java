package io.fineo.lambda.resources;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.ResultOrException;
import io.fineo.lambda.dynamo.avro.AvroDynamoReader;
import io.fineo.lambda.dynamo.iter.PageScanManager;
import io.fineo.lambda.dynamo.iter.PagingIterator;
import io.fineo.lambda.dynamo.iter.PagingScanRunner;
import io.fineo.lambda.util.EndToEndTestRunner;
import io.fineo.lambda.util.FutureWaiter;
import io.fineo.lambda.util.ResultWaiter;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class DynamoManager {
  private static final Log LOG = LogFactory.getLog(DynamoManager.class);
  private final LambdaClientProperties props;
  private final AtomicReference<SchemaStore> storeRef = new AtomicReference<>();
  private final AmazonDynamoDBAsyncClient dynamo;

  public DynamoManager(LambdaClientProperties props) {
    this.props = props;
    this.dynamo = props.getDynamo();
  }

  public void setup(FutureWaiter future) {
    future.run(() -> {
      SchemaStore store = props.createSchemaStore();
      storeRef.set(store);
      LOG.debug("Schema store creation complete!");
    });
  }

  public SchemaStore getStore() {
    return storeRef.get();
  }

  private void deleteDynamoTables(String tableNamesPrefix) {
    getTables(tableNamesPrefix).parallel().forEach(name -> {
      AmazonDynamoDBAsyncClient dynamo = props.getDynamo();
      dynamo.deleteTable(name);
      new ResultWaiter<>()
        .withDescription("Deletion dynamo table: " + name)
        .withStatusNull(() -> dynamo.describeTable(name))
        .waitForResult();
    });
  }

  public void deleteSchemaStore() {
    this.deleteDynamoTables(props.getSchemaStoreTable());
  }

  public void cleanupStoreTables() {
    String table = props.getDynamoIngestTablePrefix();
    deleteDynamoTables(table);
  }

  public void copyStoreTables(File outputDir) {
    String table = props.getDynamoIngestTablePrefix();
    getTables(table).parallel().forEach(name -> {
      try {
        // create a directory for each table
        File out = new File(outputDir, name);
        ScanRequest request = new ScanRequest(name);
        PagingScanRunner runner = new PagingScanRunner(dynamo, request, null);
        Iterable<ResultOrException<Map<String, AttributeValue>>> iter =
          () -> new PagingIterator<>(5, new PageScanManager(Lists.newArrayList(runner)));
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream
          (out)));
        for (ResultOrException<Map<String, AttributeValue>> result : iter) {
          result.doThrow();
          Map<String, AttributeValue> row = result.getResult();
          for (Map.Entry<String, AttributeValue> pair : row.entrySet()) {
            dos.writeBytes(pair.getKey() + " => " + pair.getValue() + "\n");
          }
        }
        dos.flush();
        dos.close();
      } catch (Exception e) {
        LOG.error("Failed to write dynamo data for table: " + table, e);
        throw new RuntimeException(e);
      }
    });
  }

  private Stream<String> getTables(String prefix) {
    Preconditions.checkArgument(prefix.startsWith(props.getTestPrefix()),
      "Table names have to start with the test prefix: %s. Got prefix: %s", props.getTestPrefix(),
      prefix);
    ListTablesResult tables = props.getDynamo().listTables(prefix);
    return tables.getTableNames().stream().filter(name -> name.startsWith(prefix)
    );
  }

  public List<GenericRecord> read(RecordMetadata metadata) {
    String tablePrefix = props.getDynamoIngestTablePrefix();
    ListTablesResult tablesResult = dynamo.listTables(tablePrefix);
    assertEquals(
      "Wrong number of tables after prefix" + tablePrefix + ". Got tables: " + tablesResult
        .getTableNames(), 1, tablesResult.getTableNames().size());

    AvroDynamoReader reader = new AvroDynamoReader(getStore(), dynamo, tablePrefix);
    Metric metric = getStore().getMetricMetadata(metadata);
    long ts = metadata.getBaseFields().getTimestamp();
    Range<Instant> range = Range.of(ts, ts + 1);
    ResultWaiter<List<GenericRecord>> waiter = new ResultWaiter<>()
      .withDescription("Some records to appear in dynamo")
      .withStatus(
        () -> reader.scan(metadata.getOrgID(), metric, range, null).collect(Collectors.toList()))
      .withStatusCheck(list -> ((List<GenericRecord>) list).size() > 0);
    assertTrue("Didn't get any rows from Dynamo within timeout!", waiter.waitForResult());
    return waiter.getLastStatus();
  }
}
