package io.fineo.etl.spark;

import com.google.common.collect.AbstractIterator;
import io.fineo.lambda.avro.FirehoseRecordReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import java.io.IOException;

/**
 *
 */
class RecordExtractor
  implements FlatMapFunction<Tuple2<String, PortableDataStream>, GenericRecord> {
  @Override
  public Iterable<GenericRecord> call(
    Tuple2<String, PortableDataStream> tuple) throws Exception {
    PortableDataStream stream = tuple._2();
    FSDataInputStream in = (FSDataInputStream) stream.open();
    GenericRecordReader reader = new GenericRecordReader(in);
    return () -> reader;
  }

  private static class GenericRecordReader extends AbstractIterator<GenericRecord> {

    private final FirehoseRecordReader<GenericRecord> reader;

    private GenericRecordReader(FSDataInputStream in) throws Exception {
      this.reader = FirehoseRecordReader.create(new AvroFSInput(in, in.available()));
    }

    @Override
    protected GenericRecord computeNext() {
      try {
        GenericRecord next = reader.next();
        if (next == null) {
          endOfData();
        }
        return next;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
