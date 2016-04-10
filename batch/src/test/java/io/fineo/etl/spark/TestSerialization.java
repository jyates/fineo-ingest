package io.fineo.etl.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.KryoPool;
import com.twitter.chill.avro.AvroSerializer$;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;
import scala.reflect.ClassTag;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestSerialization {

  @Test
  public void test() throws Exception {
    GenericRecord record1 = SchemaTestUtils.createRandomRecord();
    ClassTag<GenericData.Record> tag = getClassTag(GenericData.Record.class);
    KryoPool kryo = getKryo(tag, AvroSerializer$.MODULE$.GenericRecordSerializer(null, tag));
    byte[] bytes = kryo.toBytesWithClass(record1);
    assertEquals(record1, kryo.fromBytes(bytes));
  }

  public <T> KryoPool getKryo(final ClassTag<T> tag, final Serializer<T> serializer) {
    KryoInstantiator kryoInstantiator = new KryoInstantiator() {
      public Kryo newKryo() {
        Kryo k = super.newKryo();
        k.setInstantiatorStrategy(new StdInstantiatorStrategy());
        k.register(tag.runtimeClass(), serializer);
        return k;
      }
    };
    return KryoPool.withByteArrayOutputStream(1, kryoInstantiator);
  }

  private <T> ClassTag<T> getClassTag(Class<T> klass) {
    return scala.reflect.ClassTag$.MODULE$.apply(klass);
  }
}
