package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.util.Base64;
import com.google.common.collect.Lists;
import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.Record;
import io.fineo.schema.avro.AvroRecordTranslator;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.SchemaNameUtils;
import io.fineo.schema.store.SchemaStore;
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
import java.util.function.Function;
import java.util.function.Predicate;

import static io.fineo.lambda.dynamo.Schema.SORT_KEY_NAME;

/**
 * Decode a Dynamo row into a {@link GenericRecord} that can then be
 * translated into 'customer friendly' alias names via a {@link AvroRecordTranslator}.
 */
public class DynamoAvroRecordDecoder {

  private final SchemaStore store;
  private final Schema.Parser parser;

  private List<String> handledFields = Lists.newArrayList(AvroSchemaEncoder
      .BASE_FIELDS_KEY, io.fineo.lambda.dynamo.Schema.PARTITION_KEY_NAME, SORT_KEY_NAME,
    io.fineo.lambda.dynamo.Schema.ID_FIELD);
  private Predicate<Schema.Field> retainedField = field -> !handledFields.contains(field.name());

  public DynamoAvroRecordDecoder(SchemaStore store) {
    this.store = store;
    this.parser = new Schema.Parser();
  }

  public List<GenericRecord> decode(String orgId, Metric metric, Item item) {
    String schemaName =
      SchemaNameUtils.getCustomerSchemaFullName(orgId, metric.getMetadata().getCanonicalName());

    Schema schema = parser.getTypes().get(schemaName);
    if (schema == null) {
      schema = parser.parse(metric.getMetricSchema());
    }

    // extract the base fields
    long ts = item.getLong(SORT_KEY_NAME);
    Collection<String> ids = item.getStringSet(io.fineo.lambda.dynamo.Schema.ID_FIELD);
    List<GenericRecord> records = new ArrayList<>(ids.size());
    for (String id : ids) {
      records.add(parseRecord(schema, metric, ts, item, id));
    }

    return records;
  }

  private GenericRecord parseRecord(Schema schema, Metric metric, long ts, Item item, String id) {
    BaseFields base = new BaseFields();
    base.setTimestamp(ts);

    GenericData.Record record = new GenericData.Record(schema);
    record.put(AvroSchemaEncoder.BASE_FIELDS_KEY, base);

    Set<String> allFields = new HashSet<>();
    allFields.addAll(item.asMap().keySet());
    Map<String, List<String>> namesToAliases = metric.getMetadata().getCanonicalNamesToAliases();

    // set the remaining fields
    final Schema finalSchema = schema;
    schema.getFields().stream().filter(retainedField).forEach(schemaField -> {
      String name = schemaField.name();
      List<String> aliases = namesToAliases.get(name);
      String key = findKey(item, aliases);

      GenericRecord fieldRecord =
        AvroSchemaEncoder.asTypedRecord(finalSchema, name, key, new IdSpecificItemRecord(id, item));
      record.put(name, fieldRecord);
      handledFields.add(key);
      allFields.remove(name);
    });

    // copy any hidden fields
    int expectedSize = allFields.size() - handledFields.size();
    Map<String, String> unknownFields = new HashMap<>(expectedSize >= 0 ? expectedSize : 1);
    allFields.stream()
             .filter(name -> !handledFields.contains(name))
             .forEach(name -> {
               Map map = item.getMap(name);
               String value = (String) map.get(id);
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

  private class ItemRecord implements Record {
    private final Item item;

    private ItemRecord(Item item) {
      this.item = item;
    }

    @Override
    public Boolean getBooleanByField(String fieldName) {
      return item.getBoolean(fieldName);
    }

    @Override
    public Integer getIntegerByField(String fieldName) {
      return item.getInt(fieldName);
    }

    @Override
    public Long getLongByFieldName(String fieldName) {
      return item.getLong(fieldName);
    }

    @Override
    public Float getFloatByFieldName(String fieldName) {
      return item.getFloat(fieldName);
    }

    @Override
    public Double getDoubleByFieldName(String fieldName) {
      return item.getDouble(fieldName);
    }

    @Override
    public ByteBuffer getBytesByFieldName(String fieldName) {
      return item.getByteBuffer(fieldName);
    }

    @Override
    public String getStringByField(String fieldName) {
      return item.getString(fieldName);
    }

    @Override
    public Collection<String> getFieldNames() {
      return item.asMap().keySet();
    }

    @Override
    public Iterable<Map.Entry<String, Object>> getFields() {
      return item.asMap().entrySet();
    }

    @Override
    public Object getField(String name) {
      return item.get(name);
    }
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
      return null;
    }

    @Override
    public Object getField(String name) {
      return null;
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

  private Function<ByteBuffer, String> stringEncode = buffer -> {
    byte[] actual = new byte[buffer.remaining()];
    buffer.get(actual);
    return Base64.encodeAsString(actual);
  };
}
