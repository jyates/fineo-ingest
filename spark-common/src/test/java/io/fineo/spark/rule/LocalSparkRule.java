package io.fineo.spark.rule;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.ExternalResource;

/**
 * Use {@link LocalSpark} as a junit {@link ExternalResource}
 */
public class LocalSparkRule extends ExternalResource {

  private final LocalSpark spark;

  public LocalSparkRule() {
    this.spark = new LocalSpark();
  }

  public LocalSparkRule(ConfLoader loader){
    this.spark = new LocalSpark(loader);
  }

  @Override
  public void before() {
    spark.setup();
  }

  @Override
  public void after() {
    spark.stop();
  }

  public SparkConf conf() {
    return spark.conf();
  }

  public SparkContext sc() {
    return spark.sc();
  }

  public JavaSparkContext jsc() {
    return spark.jsc();
  }
}
