package io.fineo.lambda.util;

/**
 * A {@link java.util.function.Supplier} that can also throw an exception
 */
@FunctionalInterface
public interface ThrowingSupplier<T> {
  T a() throws Exception;
}
