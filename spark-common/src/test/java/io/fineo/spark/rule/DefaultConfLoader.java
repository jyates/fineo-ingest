package io.fineo.spark.rule;

import io.fineo.spark.avro.AvroSparkUtils;
import org.apache.spark.SparkConf;

public class DefaultConfLoader implements ConfLoader {
  @Override
  public void load(SparkConf conf) {
    AvroSparkUtils.setKyroAvroSerialization(conf);
  }
}
