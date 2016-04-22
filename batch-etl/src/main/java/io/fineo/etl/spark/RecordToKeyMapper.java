package io.fineo.etl.spark;

import io.fineo.schema.avro.RecordMetadata;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Map a record into a set of {@link RecordKey} and {@link GenericRecord}
 */
public class RecordToKeyMapper implements PairFlatMapFunction<GenericRecord, RecordKey, GenericRecord> {

  @Override
  public Iterable<Tuple2<RecordKey, GenericRecord>> call(GenericRecord record) throws Exception {
    RecordMetadata metadata = RecordMetadata.get(record);
    RecordKey key = new RecordKey(metadata);
    List<Tuple2<RecordKey, GenericRecord>> fields = new ArrayList<>(2);
    fields.add(new Tuple2<>(key.known(), record));
    key = key.unknown();
    if (key != null) {
      fields.add(new Tuple2<>(key, record));
    }
    return () -> fields.iterator();
  }
}
