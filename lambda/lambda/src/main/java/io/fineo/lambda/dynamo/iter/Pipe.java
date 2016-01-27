package io.fineo.lambda.dynamo.iter;

import java.util.Collection;

/**
 *
 */
public interface Pipe<T> {

  public void add(T t);

  public void addAll(Collection<T> ts);
}
