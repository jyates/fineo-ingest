package io.fineo.lambda.handle.staged;

import com.google.inject.Inject;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 *
 */
public class RecordToDynamoHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RecordToDynamoHandler.class);
  private final AvroToDynamoWriter dynamo;

  @Inject
  public RecordToDynamoHandler(AvroToDynamoWriter dynamo) {
    this.dynamo = dynamo;
  }

  public void handle(Iterator<GenericRecord> iter) {
    while (iter.hasNext()) {
      GenericRecord record = iter.next();
      LOG.info("[Dynamo] Writing record: {}", record);
      this.dynamo.write(record);
    }
  }

  public MultiWriteFailures<GenericRecord, ?> flush() {
    // get any failed writes and flushSingleEvent them into the right firehose for failures
    return this.dynamo.flush();
  }
}
