package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.util.Base64;
import com.google.common.collect.Lists;
import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.AvroRecordTranslator;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.SchemaNameUtils;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
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
    .BASE_FIELDS_KEY, io.fineo.lambda.dynamo.Schema.PARTITION_KEY_NAME, SORT_KEY_NAME, io.fineo.lambda.dynamo.Schema.MARKER);
  private Predicate<Schema.Field> retainedField = field -> !handledFields.contains(field.name());

  public DynamoAvroRecordDecoder(SchemaStore store) {
    this.store = store;
    this.parser = new Schema.Parser();
  }

  public GenericRecord decode(String orgId, Metric metric, Item item) {
    String schemaName =
      SchemaNameUtils.getCustomerSchemaFullName(orgId, metric.getMetadata().getCanonicalName());

    Schema schema = parser.getTypes().get(schemaName);
    if (schema == null) {
      schema = parser.parse(metric.getMetricSchema());
    }
    GenericData.Record record = new GenericData.Record(schema);

    // extract the base fields
    long ts = item.getLong(SORT_KEY_NAME);
    BaseFields base = new BaseFields();
    base.setTimestamp(ts);
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
        AvroSchemaEncoder.asTypedRecord(finalSchema, name, key,
          // its unfortunate that we do it this way - I'd love to not expose the internals of the
          // record, but eh, what can you do? #startup
          cast(schemaField.schema().getFields().get(1), item, key));
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
               String value = item.getString(name);
               unknownFields.put(name, value);
             });
    base.setUnknownFields(unknownFields);

    return record;
  }

  private String findKey(Item row,
    List<String> aliasNames) {
    for (String name : aliasNames) {
      Object value = row.get(name);
      if (value != null) {
       return name;
      }
    }
    throw new IllegalStateException(
      "Got a row for which no alias matches! Row: " + row + ", aliases: " + aliasNames);
  }

  private Object cast(Schema.Field field, Item item, String key) {
    switch (field.schema().getType()) {
      case STRING:
        return item.getString(key);
      case BYTES:
        return item.getBinary(key);
      case INT:
        return item.getInt(key);
      case LONG:
        return item.getLong(key);
      case FLOAT:
        return item.getFloat(key);
      case DOUBLE:
        return item.getDouble(key);
      case BOOLEAN:
        return item.getBOOL(key);
      default:
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
