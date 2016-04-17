package io.fineo.etl.spark;

import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;


public class AvroSparkUtils {
  static DataType getSparkType(Schema.Field field) {
    switch (field.schema().getType()) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case STRING:
        return DataTypes.StringType;
      case BYTES:
        return DataTypes.BinaryType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case DOUBLE:
        return DataTypes.DoubleType;
      default:
        throw new IllegalArgumentException("No spark type available for: " + field);
    }
  }
}
