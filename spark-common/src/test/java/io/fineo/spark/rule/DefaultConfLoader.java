package io.fineo.spark.rule;

import io.fineo.etl.AvroKyroRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

public class DefaultConfLoader implements ConfLoader {
  @Override
  public void load(SparkConf conf) {
    conf.set("spark.serializer", KryoSerializer.class.getName());
    conf.set("spark.kryo.registrator", AvroKyroRegistrator.class.getName());
  }
}
