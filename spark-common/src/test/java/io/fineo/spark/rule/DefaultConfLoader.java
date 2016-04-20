package io.fineo.spark.rule;

import org.apache.spark.SparkConf;

public class DefaultConfLoader implements ConfLoader {
  @Override
  public void load(SparkConf conf) {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.set("spark.kryo.registrator", "io.fineo.etl.AvroKyroRegistrator");
  }
}
