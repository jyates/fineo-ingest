package io.fineo.lambda;

import com.google.common.base.Preconditions;
import io.fineo.lambda.avro.FirehoseRecordWriter;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Helper utility for lambda functions to send errors, if any, to a firehose stream
 */
public class FailureHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FailureHandler.class);

  public static void handle(MultiWriteFailures<GenericRecord, ?> failures,
    Supplier<IFirehoseBatchWriter> creator) throws IOException {
    if (!failures.any()) {
      LOG.trace("No commit failures on invocation!");
      return;
    }

    FirehoseRecordWriter writer = new FirehoseRecordWriter();
    IFirehoseBatchWriter errors = creator.get();
    List<GenericRecord> failedRecords = getFailedRecords(failures);
    LOG.trace("{} records failed to commit correctly", failedRecords.size());
    for (GenericRecord failed : failedRecords) {
      errors.addToBatch(writer.write(failed));
    }
    LOG.trace("Flushing failed records");
    errors.flush();
  }

  public static List<GenericRecord> getFailedRecords(MultiWriteFailures<GenericRecord, ?>
    failures) {
    Preconditions.checkNotNull(failures.getActions());
    return failures.getActions().parallelStream()
                   .map(handler -> handler.getBaseRecord())
                   .collect(Collectors.toList());
  }
}
