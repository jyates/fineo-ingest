package io.fineo.lambda.handle;

import java.io.IOException;

@FunctionalInterface
public interface LambdaHandler<T> {

  void handle(T event) throws IOException;
}
