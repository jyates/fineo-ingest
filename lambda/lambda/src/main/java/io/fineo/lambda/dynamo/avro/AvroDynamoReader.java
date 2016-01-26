package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.dynamo.DynamoTableManager;
import io.fineo.lambda.dynamo.FScanRequest;
import io.fineo.lambda.dynamo.MultiDynamoScanIterator;
import io.fineo.lambda.dynamo.Range;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Read records from Dynamo
 */
public class AvroDynamoReader {

  private final AmazonDynamoDBAsyncClient client;
  private final SchemaStore store;
  private final DynamoTableManager tableManager;

  public AvroDynamoReader(SchemaStore store, AmazonDynamoDBAsyncClient client, String prefix) {
    this.store = store;
    this.client = client;
    this.tableManager = new DynamoTableManager(client, prefix);
  }

  /**
   * Scan all records for an org within a given time range.
   *
   * @return
   */
  public Stream<GenericRecord> scan(String orgId, Range<Instant> range) {
    DynamoAvroRecordDecoder decoder = new DynamoAvroRecordDecoder(store);
    AttributeValue partitionKey = Schema.getPartitionKey(orgId, "");

    return scan(range, partitionKey, result -> decoder.decode(orgId,
      result));
  }

  public Stream<GenericRecord> scan(String orgId, String aliasMetricName, Range<Instant> range) {
    AvroSchemaManager manager = new AvroSchemaManager(store, orgId);
    DynamoAvroRecordDecoder decoder = new DynamoAvroRecordDecoder(store);
    Metric metric = manager.getMetricInfo(aliasMetricName);
    String canonicalName = metric.getMetadata().getCanonicalName();
    AttributeValue partitionKey = Schema.getPartitionKey(orgId, canonicalName);
    return scan(range, partitionKey, result -> decoder.decode(metric, result));
  }

  public Function<Instant, Map<String, AttributeValue>> getStartKeys(AttributeValue partitionKey) {
    return instant -> {
      Map<String, AttributeValue> exclusiveStart = new HashMap<>(2);
      exclusiveStart.put(Schema.PARTITION_KEY_NAME, partitionKey);
      exclusiveStart.put(Schema.SORT_KEY_NAME, Schema.getSortKey(instant.toEpochMilli()));
      return exclusiveStart;
    };
  }


  public Stream<GenericRecord> scan(Range<Instant> range, AttributeValue
    stringPartitionKey, Function<Map<String, AttributeValue>,
    GenericRecord> translator) {
    // get the potential tables that match the range
    List<Pair<String, Range<Instant>>> tables = tableManager.getExistingTableNames(range);
    // get a scan across each table
    List<FScanRequest> requests = new ArrayList<>(tables.size());

    String stop = stringPartitionKey.getS()+"0";
    Function<Instant, Map<String, AttributeValue>> rangeCreator = getStartKeys(stringPartitionKey);
    for (Pair<String, Range<Instant>> table : tables) {
      FScanRequest request = new FScanRequest(table.getKey());
      request.setExclusiveStartKey(rangeCreator.apply(table.getValue().getStart()));
      request.setConsistentRead(true);
      request.setStopKey(stop);
      requests.add(request);
    }

    // create an iterable around all the requests
    Iterable<Map<String, AttributeValue>> iter =
      () -> new MultiDynamoScanIterator(client, requests);
    return StreamSupport.stream(iter.spliterator(), false).map(translator);
  }
}