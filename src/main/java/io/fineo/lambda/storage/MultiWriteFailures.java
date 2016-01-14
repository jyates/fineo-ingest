package io.fineo.lambda.storage;

import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Any failures that have accumulated since the last time that 'flush' was called
 */
public class MultiWriteFailures {

  private final List<AvroToDynamoWriter.UpdateItemHandler> actions;

  public MultiWriteFailures(List<AvroToDynamoWriter.UpdateItemHandler> failed) {
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

  public List<GenericRecord> getFailedRecords() {
    return actions.parallelStream()
                  .map(handler -> handler.getBaseRecord())
                  .collect(Collectors.toList());
  }
}
