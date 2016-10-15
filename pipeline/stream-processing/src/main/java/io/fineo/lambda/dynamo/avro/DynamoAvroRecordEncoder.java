package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;

import static org.apache.commons.lang3.tuple.MutablePair.of;

public class DynamoAvroRecordEncoder {

  static Pair<String, AttributeValue> convertField(GenericData.Record value) {
    Pair<String, AttributeValue> pair = of((String) value.get(0), null);
    Object recordValue = value.get(1);
    if(recordValue == null){
      return null;
    }
    org.apache.avro.Schema.Field field = value.getSchema().getField("value");
    switch (field.schema().getType()) {
      case STRING:
        String s = String.valueOf(recordValue);
        // dynamo only supports non-zero length string fields
        if(s.length() == 0){
          return null;
        }
        pair.setValue(new AttributeValue(s));
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
