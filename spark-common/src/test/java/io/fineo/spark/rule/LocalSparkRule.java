package io.fineo.spark.rule;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.ExternalResource;

/**
 *
 */
public class LocalSparkRule extends ExternalResource {
  private static transient SparkContext _sc;
  private static transient JavaSparkContext _jsc;
  private final ConfLoader loader;
  protected boolean initialized = false;
  private static SparkConf _conf = new SparkConf().setMaster("local[4]").setAppName("magic");

  public SparkConf conf() {
    return _conf;
  }

  public SparkContext sc() {
    return _sc;
  }

  public JavaSparkContext jsc() {
    return _jsc;
  }

  public LocalSparkRule() {
    this(new DefaultConfLoader());
  }

  public LocalSparkRule(ConfLoader loader) {
    this.loader = loader;
  }

  @Override
  public void before() {
    initialized = (_sc != null);

    if (!initialized) {
      if (this.loader != null) {
        loader.load(conf());
      }
      _sc = new SparkContext(conf());
      _jsc = new JavaSparkContext(_sc);
    }
  }

  @Override
  public void after() {
    _sc.stop();
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port");
    _sc = null;
    _jsc = null;
  }
}
