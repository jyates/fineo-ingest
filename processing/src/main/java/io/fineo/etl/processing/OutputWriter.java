package io.fineo.etl.processing;

import io.fineo.lambda.aws.MultiWriteFailures;

/**
 *
 */
public interface OutputWriter<T> {

  void write(T obj);

  MultiWriteFailures commit();
}
