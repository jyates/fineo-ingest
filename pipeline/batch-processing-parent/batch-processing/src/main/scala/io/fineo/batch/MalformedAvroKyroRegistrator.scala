package io.fineo.batch

import com.twitter.chill.Kryo
import com.twitter.chill.avro.AvroSerializer
import io.fineo.etl.AvroKyroRegistrator
import io.fineo.internal.customer.Malformed

class MalformedAvroKyroRegistrator extends AvroKyroRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)
    kryo.register(classOf[Malformed],
      AvroSerializer.GenericRecordSerializer(Malformed.getClassSchema))
  }
}
