package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.Iterators;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.Pair;
import io.fineo.schema.Record;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

  public Iterable<GenericRecord> scan(String orgId, String aliasMetricName, Range<Instant> range) {
    AvroSchemaManager manager = new AvroSchemaManager(store, orgId);
    AvroSchemaEncoder encoder = manager.encode(aliasMetricName);
    Metric metric = manager.getMetricInfo(aliasMetricName);
    String canonicalName = metric.getMetadata().getCanonicalName();

    // get the potential tables that match the range
    List<Pair<String, Range<Instant>>> tables = tableManager.getExistingTableNames(range);
    // get a scan across each table
    List<ScanRequest> requests = new ArrayList<>(tables.size());
    Map<String, AttributeValue> exclusiveStart;
    AttributeValue partitionKey = AvroToDynamoWriter
      .getPartitionKey(orgId, canonicalName);

    for (Pair<String, Range<Instant>> table : tables) {
      ScanRequest request = new ScanRequest(table.getKey());
      exclusiveStart = new HashMap<>(2);
      exclusiveStart.put(AvroToDynamoWriter.PARTITION_KEY_NAME, partitionKey);
      exclusiveStart.put(AvroToDynamoWriter.SORT_KEY_NAME, AvroToDynamoWriter.getSortKey(table
        .getValue().getStart().toEpochMilli()));
      request.setExclusiveStartKey(exclusiveStart);
      request.setConsistentRead(true);
      requests.add(request);
    }

    // create an iterable around all the requests
    return () -> Iterators.transform(new MultiDynamoScanIterator(client, requests),
      (result) -> encoder.encode(new AttributeValueBackedRecord(result)));
  }

  private class AttributeValueBackedRecord implements Record {
    private final Map<String, AttributeValue> map;

    public AttributeValueBackedRecord(Map<String, AttributeValue> map) {
      this.map = map;
    }

    @Override
    public Boolean getBooleanByField(String fieldName) {
      return get(fieldName).getBOOL();
    }

    @Override
    public Integer getIntegerByField(String fieldName) {
      return Integer.parseInt(get(fieldName).getN());
    }

    @Override
    public Long getLongByFieldName(String fieldName) {
      return Long.parseLong(get(fieldName).getN());
    }

    @Override
    public Float getFloatByFieldName(String fieldName) {
      return Float.parseFloat(get(fieldName).getN());
    }

    @Override
    public Double getDoubleByFieldName(String fieldName) {
      return Double.parseDouble(get(fieldName).getN());
    }

    @Override
    public ByteBuffer getBytesByFieldName(String fieldName) {
      return get(fieldName).getB();
    }

    @Override
    public String getStringByField(String fieldName) {
      return get(fieldName).getS();
    }

    @Override
    public Collection<String> getFieldNames() {
      return this.map.keySet();
    }

    @Override
    public Iterable<Map.Entry<String, Object>> getFields() {
      return map.entrySet().stream().map(
        entry -> new AbstractMap.SimpleEntry<String, Object>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toSet());
    }

    @Override
    public Object getField(String name) {
      return null;
    }

    private AttributeValue get(String name) {
      return map.get(name);
    }
  }
}