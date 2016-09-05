package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.Iterators;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.fineo.lambda.dynamo.Schema.PARTITION_KEY_NAME;
import static io.fineo.lambda.dynamo.Schema.SORT_KEY_NAME;

/**
 * Read records from Dynamo
 */
public class AvroDynamoReader {

  private static final Logger LOG = LoggerFactory.getLogger(AvroDynamoReader.class);
  private final AmazonDynamoDBAsyncClient client;
  private final SchemaStore store;
  private final DynamoTableTimeManager tableManager;
  private int prefetchSize;

  @Inject
  public AvroDynamoReader(SchemaStore store, AmazonDynamoDBAsyncClient client,
    DynamoTableTimeManager manager) {
    this.store = store;
    this.client = client;
    this.tableManager = manager;
  }

  public void setPrefetchSize(int prefetchSize) {
    this.prefetchSize = prefetchSize;
  }

  public Stream<GenericRecord> scanMetricAlias(String orgId, String aliasMetricName,
    Range<Instant> range) throws SchemaNotFoundException {
    StoreClerk clerk = new StoreClerk(store, orgId);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(aliasMetricName);
    return scan(orgId, metric, range);
  }

  public Stream<GenericRecord> scan(String orgId, StoreClerk.Metric metric, Range<Instant> range) {
    String canonicalName = metric.getMetricId();
    AttributeValue partitionKey = Schema.getPartitionKey(orgId, canonicalName);
    DynamoAvroRecordDecoder decoder = new DynamoAvroRecordDecoder();
    return scanMetricAlias(range, partitionKey, result -> decoder.decode(orgId, metric, result));
  }

  private Stream<GenericRecord> scanMetricAlias(Range<Instant> range, AttributeValue
    stringPartitionKey, Function<Item, List<GenericRecord>> translator) {
    // get the potential tables that match the range
    List<Pair<String, Range<Instant>>> tables = tableManager.getCoveringTableNames(range);
    LOG.debug("Scanning tables: " + tables);

    DynamoDB dynamo = new DynamoDB(client);
    List<Iterable<Item>> iters = new ArrayList<>();
    String startPartition = stringPartitionKey.getS();
    String stop = startPartition + "0";
    String start = startPartition.substring(0, startPartition.length() - 1);
    Function<Range<Instant>, Long> sortStart =
      // read prefix has to start *before* the desired read
      r -> Long.parseLong(Schema.getSortKey(range.getStart().toEpochMilli()).getN()) - 1;
    for (Pair<String, Range<Instant>> table : tables) {
      Table dt = dynamo.getTable(table.getKey());
      ScanSpec spec = new ScanSpec();
      spec.withConsistentRead(true);
      spec.withExclusiveStartKey(PARTITION_KEY_NAME, startPartition,
        SORT_KEY_NAME, sortStart.apply(table.getValue()));
      Iterable<Item> iter = dt.scan(spec);
      Iterator<Item> itemIter = iter.iterator();
      // wrap the iterator to stop when we hit the end key
      iters.add(() -> Iterators.whereStop(itemIter,
        item -> item.getString(PARTITION_KEY_NAME).compareTo(stop) <= 0));
    }

    return StreamSupport.stream(Iterables.concat(iters).spliterator(), false).map(translator)
                        .flatMap(List::stream);
  }
}
