package io.fineo.etl.spark.util;

import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;


public class AvroSparkUtils {
  public static DataType getSparkType(Schema.Field field) {
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

  /**
   * We cannot serialize avro classes (e.g. avro lists) with the Java serializer, which is
   * currently the only serializer that is supported for serializing closures
   *
   * @param map multimap of string -> string
   * @return the same map, with as many of the same values as possible.
   */
  public static Map<String, List<String>> removeUnserializableAvroTypesFromMap(
    Map<String, List<String>> map) {
    Map<String, List<String>> ret = new HashMap<>(map.size());
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      ret.put(entry.getKey(), newArrayList(entry.getValue()));
    }
    return ret;
  }
}
