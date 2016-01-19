package io.fineo.lambda.aws;

import io.fineo.lambda.storage.AvroToDynamoWriter;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Any failures that have accumulated since the last time that 'flush' was called
 */
public class MultiWriteFailures<T> {

  private final List<AwsAsyncRequest<T, ?>> actions;

  public MultiWriteFailures(List<AwsAsyncRequest<T, ?>> failed) {
    this.actions = failed;
  }

  /**
   * Check to see if all the results are successful. If not, use
   *
   * @return
   */
  public boolean any() {
    return actions.size() > 0;
  }

  public List<AwsAsyncRequest<T, ?>> getActions() {
    return actions;
  }
}
