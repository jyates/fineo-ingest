package io.fineo.lambda;

/**
 * A {@link java.util.function.Supplier} that can also throw an exception
 */
@FunctionalInterface
interface ThrowingSupplier<T> {
  T a() throws Exception;
}
