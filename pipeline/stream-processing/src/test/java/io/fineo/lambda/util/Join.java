package io.fineo.lambda.util;

import java.util.function.BinaryOperator;

/**
 * Stream-based version of Guava's Joiner
 */
public class Join implements BinaryOperator<String> {
  private final String separator;

  public Join(String s) {
    this.separator = s;
  }

  public static Join on(String s) {
    return new Join(s);
  }

  @Override
  public String apply(String s, String s2) {
    if (s == null) {
      return s2;
    }
    return s + separator + s2;
  }
}
