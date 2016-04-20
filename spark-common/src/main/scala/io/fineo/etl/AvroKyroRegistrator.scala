package io.fineo.etl

import com.twitter.chill.Kryo
import com.twitter.chill.avro.AvroSerializer
import org.apache.avro.generic.GenericData
import org.apache.spark.serializer.KryoRegistrator

class AvroKyroRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[GenericData.Record] , AvroSerializer.GenericRecordSerializer(null))
  }
}
