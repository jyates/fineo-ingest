package io.fineo.spark.avro;

import io.fineo.etl.AvroKyroRegistrator;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.spark.sql.types.DataTypes.createStructField;


public class AvroSparkUtils {

  public static void setKyroAvroSerialization(SparkConf conf) {
    conf.set("spark.serializer", KryoSerializer.class.getName());
    conf.set("spark.kryo.registrator", AvroKyroRegistrator.class.getName());
  }

  public static StructField handleField(Schema.Field field) {
    return createStructField(field.name(), getSparkType(field), true);
  }

  public static DataType getSparkType(Schema.Field field) {
    Schema.Type type = field.schema().getType();
    switch(type){
      case ARRAY:
        Schema element = field.schema().getElementType();
        Schema.Field arrayField = new Schema.Field(element.getName(), element, element.getDoc(),
          null);
        DataType arrayType = getSparkType(arrayField);
        return new ArrayType(arrayType, true);
      case RECORD:
        return handleRecordField(field, field.schema());
      default:
        return getSimpleSparkType(type);
    }
  }

  private static DataType handleRecordField(Schema.Field field, Schema fieldSchema) {
    // create a more complex data type based on the field types
    List<StructField> fields = new ArrayList<>();
    fieldSchema.getFields().stream().forEach(f -> {
      StructField sf = handleField(f);
      fields.add(sf);
    });
    return new StructType(fields.toArray(new StructField[0]));
  }

  public static DataType getSimpleSparkType(Schema.Type type) {
    switch (type) {
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
      // should have already been handled
      case ARRAY:
      default:
        throw new IllegalArgumentException("No spark type available for: " + type);
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
