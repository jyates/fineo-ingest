package io.fineo.etl.spark.fs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 *
 */
public class RddUtils {

  private RddUtils(){}

  public static <A, B> JavaRDD getRddByKey(JavaPairRDD<A, Iterable<B>> pairRDD, A key) {
    return pairRDD.filter(v -> v._1().equals(key)).values().flatMap(tuples -> tuples);
  }
}
