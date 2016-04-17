package io.fineo.spark.rule;

import org.apache.spark.SparkConf;

@FunctionalInterface
public interface ConfLoader {

  void load(SparkConf conf);
}
