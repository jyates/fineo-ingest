package io.fineo.spark.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static io.fineo.spark.avro.AvroSparkUtils.getSparkType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

/**
 * Function to convert GenericRecords into {@link Row Rows} based on the schema
 */
public abstract class RowConverter implements Function<GenericRecord, Row>, Serializable {

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
    List<Object> fields = new ArrayList<>();
    initFields(fields, record);
    schemaStream()
      .map(field -> field.name())
      .forEach(name -> {
        Object fieldRecord = record.get(name);
        fields.add(getValue(name, fieldRecord));
      });

    return RowFactory.create(fields.toArray());
  }

  protected Object getValue(String name, Object field) {
    return field;
  }

  protected Stream<Schema.Field> filterSchema(Stream<Schema.Field> schema) {
    return schema;
  }

  protected void initFields(List<Object> fields, GenericRecord record) {
    // noop
  }

  private Stream<Schema.Field> schemaStream() {
    Schema schema = getSchema();
    return filterSchema(schema.getFields().stream()).sequential();
  }

  public StructType getStruct() {
    List<StructField> fields = new ArrayList<>();
    initStructFields(fields);
    schemaStream()
      .forEach(field -> {
        Schema fieldSchema = field.schema();
        StructField f;
        switch (fieldSchema.getType()) {
          case RECORD:
            f = handleRecordField(field, fieldSchema);
            break;
          default:
            f = handleField(field);
        }
        if (f != null) {
          fields.add(f);
        }
      });
    return createStructType(fields);
  }

  protected abstract StructField handleRecordField(Schema.Field field, Schema fieldSchema);

  protected StructField handleField(Schema.Field field) {
    return createStructField(field.name(), getSparkType(field), true);
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
