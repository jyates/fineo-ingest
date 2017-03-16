package io.fineo.lambda.handle.staged;

import com.google.inject.Inject;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.handle.TenantBoundFineoException;
import io.fineo.schema.avro.RecordMetadata;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Light wrapper around the actual writer. Mostly used to associate an api key with any failed
 * request(s).
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
      RecordMetadata metadata = RecordMetadata.get(record);
      try {
        this.dynamo.write(record);
      } catch (Exception e) {
        String key = metadata.getOrgID();
        if (key == null) {
          throw e;
        } else {
          throw new TenantBoundFineoException("Failed to write record to dynamo.", e.getCause(),
            key, metadata.getBaseFields().getWriteTime());
        }
      }
    }

  }

  public MultiWriteFailures<GenericRecord, ?> flush() {
    // get any failed writes and flushSingleEvent them into the right firehose for failures
    return this.dynamo.flush();
  }
}
