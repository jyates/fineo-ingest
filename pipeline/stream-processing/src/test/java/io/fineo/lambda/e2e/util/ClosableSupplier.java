package io.fineo.lambda.e2e.util;

import java.io.Closeable;
import java.util.function.Supplier;

public abstract class ClosableSupplier<T> implements Supplier<T>, Closeable {
}
