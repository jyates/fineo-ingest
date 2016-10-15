package io.fineo.etl.spark;

import io.fineo.internal.customer.Metric;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SerializableMetric implements Serializable {
  private Metric metric;

  public SerializableMetric(Metric underlyingMetric) {
    this.metric = underlyingMetric;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    SpecificDatumWriter<Metric> writer = new SpecificDatumWriter<>(metric.getSchema());
    Encoder encode = new EncoderFactory().binaryEncoder(out, null);
    writer.write(metric, encode);
    encode.flush();

  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    SpecificDatumReader<Metric> reader = new SpecificDatumReader<>(Metric.getClassSchema());
    metric = reader.read(null, new DecoderFactory().binaryDecoder(in, null));
  }

  public Metric get() {
    return this.metric;
  }
}
