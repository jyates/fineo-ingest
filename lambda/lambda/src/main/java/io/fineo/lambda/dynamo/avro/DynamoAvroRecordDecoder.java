package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.Base64;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.AvroRecordTranslator;
import io.fineo.schema.avro.AvroSchemaEncoder;
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

/**
 * Decode a Dynamo row into a {@link GenericRecord} that can then be
 * translated into 'customer friendly' alias names via a {@link AvroRecordTranslator}.
 */
public class DynamoAvroRecordDecoder {

  private final SchemaStore store;
  private final Schema.Parser parser;

  private List<String> skippedFields = Lists.newArrayList(AvroSchemaEncoder
    .BASE_FIELDS_KEY, io.fineo.lambda.dynamo.avro.Schema.PARTITION_KEY_NAME, io.fineo.lambda
    .dynamo.avro.Schema.SORT_KEY_NAME);
  private Predicate<Schema.Field> isSkipField = field -> skippedFields.contains(field.name());

  public DynamoAvroRecordDecoder(SchemaStore store) {
    this.store = store;
    this.parser = new Schema.Parser();
  }

  public GenericRecord decode(String orgId, Map<String, AttributeValue> row) {
    Metadata org = store.getSchemaTypes(orgId);
    String partitionKey = row.get(io.fineo.lambda.dynamo.avro.Schema.PARTITION_KEY_NAME).getS();
    // skip past the org in the key
    String metricID = partitionKey.substring(orgId.length());
    Metric metric = store.getMetricMetadataFromAlias(org, metricID);
    return decode(metric, row);
  }

  public GenericRecord decode(Metric metric, Map<String, AttributeValue> row) {
    Schema schema = parser.parse(metric.getMetricSchema());
    GenericData.Record record = new GenericData.Record(schema);

    // extract the base fields
    long ts = Long.valueOf(row.get(io.fineo.lambda.dynamo.avro.Schema.SORT_KEY_NAME).getN());
    BaseFields base = new BaseFields();
    base.setTimestamp(ts);
    record.put(AvroSchemaEncoder.BASE_FIELDS_KEY, base);

    Set<String> allFields = new HashSet<>();
    allFields.addAll(row.keySet());

    // set the remaining fields
    schema.getFields().stream().filter(isSkipField.negate()).forEach(field -> {
      String name = field.name();
      record.put(field.name(), cast(field, row.get(name)));
      allFields.remove(name);
    });

    // copy any hidden fields
    Map<String, String> unknownFields = new HashMap<>(allFields.size());
    allFields.stream().forEach(name -> {
      String value = getFieldAsString(row.get(name));
      unknownFields.put(name, value);
    });
    base.setUnknownFields(unknownFields);

    return record;
  }

  private Object cast(Schema.Field field, AttributeValue value) {
    switch (field.schema().getType()) {
      case STRING:
        return value.getS();
      case BYTES:
        return value.getB();
      case INT:
        return Integer.valueOf(value.getN());
      case LONG:
        return Long.parseLong(value.getN());
      case FLOAT:
        return Float.parseFloat(value.getN());
      case DOUBLE:
        Double.parseDouble(value.getN());
      case BOOLEAN:
        return value.getBOOL();
      default:
        return null;
    }
  }

  private String getFieldAsString(AttributeValue value) {
    if (value.getS() != null)
      return value.getS();
    else if (value.getN() != null)
      return value.getN();
    else if (value.getBOOL() != null)
      return value.getBOOL().toString();
    else if (value.getSS() != null)
      return Joiner.on(",").join(value.getSS());
    else if (value.getNS() != null)
      return Joiner.on(",").join(value.getNS());
    else if (value.getBS() != null)
      return Joiner.on(",").join(value.getBS().stream().map(stringEncode).toArray());
    else if (value.getM() != null)
      throw new IllegalArgumentException("Map type not supported!");
    else if (value.getL() != null)
      throw new IllegalArgumentException("List of types not supported!");
    else if (value.getNULL() != null)
      return "";
    else if (value.getB() != null)
      return stringEncode.apply(value.getB());
    throw new IllegalArgumentException("All attribute fields are null!");
  }

  private Function<ByteBuffer, String> stringEncode = buffer -> {
    byte[] actual = new byte[buffer.remaining()];
    buffer.get(actual);
    return Base64.encodeAsString(actual);
  };
}