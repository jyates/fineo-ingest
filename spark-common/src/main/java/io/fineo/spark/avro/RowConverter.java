package io.fineo.spark.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.spark.sql.types.DataTypes.createStructType;

/**
 * Function to convert GenericRecords into {@link Row Rows} based on the schema
 */
public abstract class RowConverter implements Function<GenericRecord, Row>, Serializable {

  private static final Object[] EMPTY = new Object[0];
  private static final Logger LOG = LoggerFactory.getLogger(RowConverter.class);
  private final String schemaString;
  private transient Schema schema;
  private transient Schema.Parser parser;

  public RowConverter(String schemaString) {
    this.schemaString = schemaString;
  }

  public RowConverter(Schema schema) {
    this(schema.toString());
  }

  @Override
  public Row call(GenericRecord record) throws Exception {
    Object[] arr = convert(record, record.getSchema());
    return RowFactory.create(arr);
  }

  private Object[] convert(GenericRecord record, Schema schema) {
    List<Object> ret = new ArrayList<>(schema.getFields().size());
    schema.getFields().forEach(field -> {
      String name = field.name();
      Schema fSchema = field.schema();
      Object value = getValue(name, record.get(name));
      switch (fSchema.getType()) {
        case ARRAY:
          ret.add(convertList((List<Object>) value, fSchema));
          break;
        case RECORD:
          Object[] row = convert((GenericRecord) value, fSchema);
          ret.add(RowFactory.create(row));
          break;
        default:
          ret.add(value);
          break;
      }
    });
    return ret.toArray(EMPTY);
  }


  private Object[] convertList(List<Object> list, Schema schema) {
    Schema elemSchema = schema.getElementType();
    switch (elemSchema.getType()) {
      case ARRAY:
        List<Object> arrayReturn = new ArrayList<>(list.size());
        for (Object o : list) {
          arrayReturn.add(convertList((List<Object>) o, elemSchema));
        }
        return arrayReturn.toArray(EMPTY);
      case RECORD:
        List<Object> recordReturn = new ArrayList<>(list.size());
        for (Object o : list) {
          Object[] row = convert((GenericRecord) o, elemSchema);
          recordReturn.add(RowFactory.create(row));
        }
        return recordReturn.toArray(EMPTY);
      default:
        // simple case, its just values so return just the original
        return list.toArray(EMPTY);
    }
  }

  protected Object getValue(String name, Object field) {
    return field;
  }

  protected Stream<Schema.Field> filterSchema(Stream<Schema.Field> schema) {
    return schema;
  }

  private Stream<Schema.Field> schemaStream() {
    Schema schema = getSchema();
    return filterSchema(schema.getFields().stream()).sequential();
  }

  public StructType getStruct() {
//    return (StructType) SchemaConverters.toSqlType(this.getSchema()).dataType();
    List<StructField> fields = new ArrayList<>();
    initStructFields(fields);
    schemaStream()
      .forEach(field -> {
        StructField f = handleField(field);
        if (f != null) {
          fields.add(f);
        }
      });
    return createStructType(fields);
  }

  protected StructField handleField(Schema.Field field) {
    return AvroSparkUtils.handleField(field);
  }


  protected void initStructFields(List<StructField> fields) {
    // noop
  }

  private Schema getSchema() {
    if (this.schema == null) {
      this.parser = new Schema.Parser();
      this.schema = parser.parse(schemaString);
    }

    return this.schema;
  }
}
