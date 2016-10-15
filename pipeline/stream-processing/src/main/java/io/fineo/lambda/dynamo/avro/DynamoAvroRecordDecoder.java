package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.document.Item;
import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.FieldMetadata;
import io.fineo.schema.Record;
import io.fineo.schema.avro.SchemaNameUtils;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.dynamo.Schema.ID_FIELD;
import static io.fineo.lambda.dynamo.Schema.METRIC_ORIGINAL_ALIAS_FIELD;
import static io.fineo.lambda.dynamo.Schema.PARTITION_KEY_NAME;
import static io.fineo.lambda.dynamo.Schema.SORT_KEY_NAME;
import static io.fineo.lambda.dynamo.Schema.WRITE_TIME_FIELD;
import static io.fineo.schema.store.AvroSchemaEncoder.asTypedRecord;

/**
 * Decode a Dynamo row into a {@link GenericRecord} that can then be
 * translated into 'customer friendly' alias names via a AvroRecordTranslator.
 */
public class DynamoAvroRecordDecoder {
  private final Schema.Parser parser;

  private List<String> handledFields =
    newArrayList(AvroSchemaProperties.BASE_FIELDS_KEY,
      PARTITION_KEY_NAME,
      SORT_KEY_NAME,
      ID_FIELD);
  private List<String> lazyBaseFields = newArrayList(
    WRITE_TIME_FIELD,
    METRIC_ORIGINAL_ALIAS_FIELD
  );
  private Predicate<Schema.Field> retainedField = field -> !handledFields.contains(field.name());

  public DynamoAvroRecordDecoder() {
    this.parser = new Schema.Parser();
  }

  public List<GenericRecord> decode(String orgId, StoreClerk.Metric metric, Item item) {
    String schemaName =
      SchemaNameUtils.getCustomerSchemaFullName(orgId, metric.getMetricId());

    Schema schema = parser.getTypes().get(schemaName);
    if (schema == null) {
      schema = parser.parse(metric.getUnderlyingMetric().getMetricSchema());
    }

    // extract the base fields
    long ts = item.getLong(SORT_KEY_NAME);
    Collection<String> ids = item.getStringSet(ID_FIELD);
    List<GenericRecord> records = new ArrayList<>(ids.size());
    for (String id : ids) {
      records.add(parseRecord(schema, metric, ts, item, id));
    }

    return records;
  }

  private GenericRecord parseRecord(Schema schema, StoreClerk.Metric metric, long ts, Item item,
    String id) {
    BaseFields base = new BaseFields();
    base.setTimestamp(ts);

    GenericData.Record record = new GenericData.Record(schema);
    record.put(AvroSchemaProperties.BASE_FIELDS_KEY, base);

    // lazy loaded base fields
    lazyBaseFields.stream()
                  .forEach(key -> {
                    Record idRecord = new IdSpecificItemRecord(id, item);
                    switch (key) {
                      case WRITE_TIME_FIELD:
                        base.setWriteTime(idRecord.getLongByFieldName(key));
                        break;
                      case METRIC_ORIGINAL_ALIAS_FIELD:
                        base.setAliasName(idRecord.getStringByField(key));
                        break;
                      default:
                        throw new IllegalStateException("Got unexpected handled field: " + key);
                    }
                  });

    // set the remaining fields
    Set<String> allFields = new HashSet<>();
    allFields.addAll(item.asMap().keySet());
    Map<String, FieldMetadata> namesToAliases = metric.getUnderlyingMetric().getMetadata()
                                                      .getFields();
    final Schema finalSchema = schema;
    schema.getFields().stream().filter(retainedField).forEach(schemaField -> {
      String name = schemaField.name();
      List<String> aliases = namesToAliases.get(name).getFieldAliases();
      try {
        String key = findKey(item, aliases);

        Record idRecord = new IdSpecificItemRecord(id, item);
        GenericRecord fieldRecord = asTypedRecord(finalSchema, name, key, idRecord);
        record.put(name, fieldRecord);
        handledFields.add(key);
      } catch (IllegalStateException e) {
        // means there is no value found for that key
      }
      allFields.remove(name);
    });

    // copy any hidden fields
    int expectedSize = allFields.size() - handledFields.size();
    Map<String, String> unknownFields = new HashMap<>(expectedSize >= 0 ? expectedSize : 1);
    allFields.stream()
             .filter(name -> !handledFields.contains(name))
             .filter(name -> !lazyBaseFields.contains(name))
             .forEach(name -> {
               Map map = item.getMap(name);
               String value = map.get(id).toString();
               if (value != null) {
                 unknownFields.put(name, value);
               }
             });
    base.setUnknownFields(unknownFields);
    return record;
  }

  private String findKey(Item row, List<String> aliasNames) {
    for (String name : aliasNames) {
      Map map = row.getMap(name);
      if (map != null) {
        return name;
      }
    }
    throw new IllegalStateException(
      "Got a row for which no alias matches! Row: " + row + ", aliases: " + aliasNames);
  }

  private class IdSpecificItemRecord implements Record {
    private final String id;
    private final Item item;

    private IdSpecificItemRecord(String id, Item item) {
      this.id = id;
      this.item = item;
    }

    @Override
    public Boolean getBooleanByField(String fieldName) {
      Map<String, Boolean> map = item.getMap(fieldName);
      return map.get(id);
    }

    @Override
    public Integer getIntegerByField(String fieldName) {
      return ((BigDecimal) item.getMap(fieldName).get(id)).intValue();
    }

    @Override
    public Long getLongByFieldName(String fieldName) {
      return ((BigDecimal) item.getMap(fieldName).get(id)).longValue();
    }

    @Override
    public Float getFloatByFieldName(String fieldName) {
      return ((BigDecimal) item.getMap(fieldName).get(id)).floatValue();
    }

    @Override
    public Double getDoubleByFieldName(String fieldName) {
      return ((BigDecimal) item.getMap(fieldName).get(id)).doubleValue();
    }

    @Override
    public ByteBuffer getBytesByFieldName(String fieldName) {
      return (ByteBuffer) item.getMap(fieldName).get(id);
    }

    @Override
    public String getStringByField(String fieldName) {
      return (String) item.getMap(fieldName).get(id);
    }

    @Override
    public Collection<String> getFieldNames() {
      return item.asMap().keySet();
    }

    @Override
    public Iterable<Map.Entry<String, Object>> getFields() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getField(String name) {
      return item.getMap(name).get(id);
    }
  }

//  private String getFieldAsString(AttributeValue value) {
//    if (value.getS() != null)
//      return value.getS();
//    else if (value.getN() != null)
//      return value.getN();
//    else if (value.getBOOL() != null)
//      return value.getBOOL().toString();
//    else if (value.getSS() != null)
//      return Joiner.on(",").join(value.getSS());
//    else if (value.getNS() != null)
//      return Joiner.on(",").join(value.getNS());
//    else if (value.getBS() != null)
//      return Joiner.on(",").join(value.getBS().stream().map(stringEncode).toArray());
//    else if (value.getM() != null)
//      throw new IllegalArgumentException("Map type not supported!");
//    else if (value.getL() != null)
//      throw new IllegalArgumentException("List of types not supported!");
//    else if (value.getNULL() != null)
//      return "";
//    else if (value.getB() != null)
//      return stringEncode.apply(value.getB());
//    throw new IllegalArgumentException("All attribute fields are null!");
//  }
}
