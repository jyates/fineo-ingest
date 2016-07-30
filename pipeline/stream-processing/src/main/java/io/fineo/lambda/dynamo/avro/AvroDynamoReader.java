package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.inject.Inject;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.DynamoTableTimeManager;
import io.fineo.lambda.dynamo.Range;
import io.fineo.lambda.dynamo.ResultOrException;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.lambda.dynamo.iter.PageManager;
import io.fineo.lambda.dynamo.iter.PagingIterator;
import io.fineo.lambda.dynamo.iter.ScanPager;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

  public Stream<GenericRecord> scan(String orgId, String aliasMetricName, Range<Instant> range) {
    return scan(orgId, aliasMetricName, range, null);
  }

  public Stream<GenericRecord> scan(String orgId, String aliasMetricName, Range<Instant> range,
    ScanRequest baseRequest) {
    AvroSchemaManager manager = new AvroSchemaManager(store, orgId);
    Metric metric = manager.getMetricInfo(aliasMetricName);
    return scan(orgId, metric, range, baseRequest);
  }

  public Stream<GenericRecord> scan(String orgId, Metric metric, Range<Instant> range,
    ScanRequest baseRequest) {
    String canonicalName = metric.getMetadata().getCanonicalName();
    AttributeValue partitionKey = Schema.getPartitionKey(orgId, canonicalName);
    DynamoAvroRecordDecoder decoder = new DynamoAvroRecordDecoder(store);
    return scan(range, partitionKey, result -> decoder.decode(orgId, metric, result),
      Optional.ofNullable(baseRequest));
  }

  private Function<Instant, Map<String, AttributeValue>> getStartKeys(AttributeValue partitionKey) {
    return instant -> {
      Map<String, AttributeValue> exclusiveStart = new HashMap<>(2);
      exclusiveStart.put(Schema.PARTITION_KEY_NAME, partitionKey);
      exclusiveStart.put(Schema.SORT_KEY_NAME, Schema.getSortKey(instant.toEpochMilli()));
      return exclusiveStart;
    };
  }

  private Stream<GenericRecord> scan(Range<Instant> range, AttributeValue
    stringPartitionKey, Function<Map<String, AttributeValue>,
    GenericRecord> translator, Optional<ScanRequest> baseRequest) {
    // get the potential tables that match the range
    List<Pair<String, Range<Instant>>> tables = tableManager.getCoveringTableNames(range);
    LOG.debug("Scanning tables: " + tables);
    // get a scan across each table
    List<ScanPager> scanners = new ArrayList<>(tables.size());

    String stop = stringPartitionKey.getS() + "0";
    Function<Instant, Map<String, AttributeValue>> rangeCreator = getStartKeys(stringPartitionKey);
    for (Pair<String, Range<Instant>> table : tables) {
      ScanRequest request = baseRequest.isPresent() ?
                            baseRequest.get().clone() : new ScanRequest();
      request.setTableName(table.getKey());
      request.setExclusiveStartKey(rangeCreator.apply(table.getValue().getStart()));
      request.setConsistentRead(true);
      scanners.add(new ScanPager(client, request, Schema.PARTITION_KEY_NAME, stop));
    }

    // create an iterable around all the requests
    Iterable<ResultOrException<Map<String, AttributeValue>>> iter =
      () -> new PagingIterator<>(prefetchSize, new PageManager(scanners));
    return StreamSupport.stream(iter.spliterator(), false).map(re -> {
      re.doThrow();
      return re.getResult();
    }).map(translator);
  }
}
