package io.fineo.lambda.dynamo.iter;


@FunctionalInterface
public interface VoidCallWithArg<T> {

  void call(T arg);
}
