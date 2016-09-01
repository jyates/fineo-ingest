package io.fineo.lambda.handle.staged;

import com.google.inject.Inject;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import org.apache.avro.generic.GenericRecord;

import java.util.Iterator;

/**
 *
 */
public class RecordToDynamoHandler {

  private final AvroToDynamoWriter dynamo;

  @Inject
  public RecordToDynamoHandler(AvroToDynamoWriter dynamo) {
    this.dynamo = dynamo;
  }

  public void handle(Iterator<GenericRecord> iter){
    while (iter.hasNext()) {
      this.dynamo.write(iter.next());
    }
  }

  public MultiWriteFailures<GenericRecord> flush() {
      // get any failed writes and flush them into the right firehose for failures
      return this.dynamo.flush();
    }
}
