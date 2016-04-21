package io.fineo.lambda.e2e.validation;

@FunctionalInterface
public interface TriFunction<A, B, C, RETURN> {

  public RETURN apply(A a, B b, C c);
}
