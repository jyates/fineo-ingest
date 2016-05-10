package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;

import static org.apache.commons.lang3.tuple.MutablePair.of;

public class DynamoAvroRecordEncoder {

  private static final String KNOWN_FIELD_PREFIX = "k_";
  private static final String UNKNOWN_FIELD_PREFIX = "u_";

  public static String getKnownFieldName(String avroAliasFieldName){
    return KNOWN_FIELD_PREFIX+avroAliasFieldName;
  }

  public static String getUnknownFieldName(String avroAliasFieldName){
    return UNKNOWN_FIELD_PREFIX+avroAliasFieldName;
  }

  static Pair<String, AttributeValue> convertField(GenericData.Record value) {
    Pair<String, AttributeValue> pair = of(getKnownFieldName((String) value.get(0)), null);
    Object recordValue = value.get(1);
    org.apache.avro.Schema.Field field = value.getSchema().getField("value");
    switch (field.schema().getType()) {
      case STRING:
        pair.setValue(new AttributeValue(String.valueOf(recordValue)));
        break;
      case BYTES:
        pair.setValue(new AttributeValue().withB(ByteBuffer.wrap((byte[]) recordValue)));
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        pair.setValue(new AttributeValue().withN(recordValue.toString()));
        break;
      case BOOLEAN:
        pair.setValue(new AttributeValue().withBOOL(Boolean.valueOf(recordValue.toString())));
        break;
      default:
        return null;
    }
    return pair;
  }
}
