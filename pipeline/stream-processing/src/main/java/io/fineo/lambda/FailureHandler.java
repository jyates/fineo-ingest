package io.fineo.lambda;

import com.google.common.base.Preconditions;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.avro.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Helper utility for lambda functions to send errors, if any, to a firehose stream
 */
public class FailureHandler {

  public static void handle(MultiWriteFailures<GenericRecord> failures,
    Supplier<FirehoseBatchWriter> creator) throws IOException {
    if (!failures.any()) {
      return;
    }

    FirehoseRecordWriter writer = new FirehoseRecordWriter();
    FirehoseBatchWriter errors = creator.get();
    for (GenericRecord failed : getFailedRecords(failures)) {
      errors.addToBatch(writer.write(failed));
    }
    errors.flush();
  }

  public static List<GenericRecord> getFailedRecords(MultiWriteFailures<GenericRecord> failures) {
    Preconditions.checkNotNull(failures.getActions());
    return failures.getActions().parallelStream()
                   .map(handler -> handler.getBaseRecord())
                   .collect(Collectors.toList());
  }
}
