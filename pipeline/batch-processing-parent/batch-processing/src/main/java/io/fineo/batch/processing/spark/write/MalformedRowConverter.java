package io.fineo.batch.processing.spark.write;

import io.fineo.internal.customer.Malformed;
import io.fineo.spark.avro.RowConverter;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.nio.ByteBuffer;

import static org.apache.spark.sql.types.DataTypes.createStructField;

/**
 * Row converter for {@link io.fineo.internal.customer.Malformed} records. Its special because:
 * <ol>
 * <li>'content' is converted to string from raw {@link ByteBuffer}</li>
 * </ol>
 */
public class MalformedRowConverter extends RowConverter {

  public static final String RECORD_CONTENT = "recordContent";

  public MalformedRowConverter() {
    super(Malformed.getClassSchema());
  }

  @Override
  protected Object getValue(String name, Object field) {
    if (name.equals(RECORD_CONTENT)) {
      ByteBuffer bb = (ByteBuffer) field;
      byte[] data = new byte[bb.remaining()];
      bb.get(data);
      return new String(data);
    }
    return super.getValue(name, field);
  }

  @Override
  protected StructField handleRecordField(Schema.Field field, Schema fieldSchema) {
    throw new UnsupportedOperationException("Malformed records should not have RECORD fields!");
  }

  @Override
  protected StructField handleField(Schema.Field field) {
    if (field.name().equals(RECORD_CONTENT)) {
      return createStructField(field.name(), DataTypes.StringType, true);
    }
    return super.handleField(field);
  }
}
