package io.fineo.batch.processing.spark.write;

import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Write the records to dynamo
 */
public class DynamoWriter implements VoidFunction<Iterator<GenericRecord>>, Serializable {

  private final BatchOptions props;
  private transient RecordToDynamoHandler handler;

  public DynamoWriter(BatchOptions props) {
    this.props = props;
  }

  @Override
  public void call(Iterator<GenericRecord> genericRecordIterator) throws Exception {
    getHandler().handle(genericRecordIterator);
    MultiWriteFailures failed = handler.flush();
    if (failed.any()) {
      throw new RuntimeException("Failed to store some records!" + failed.getActions());
    }
  }

  private RecordToDynamoHandler getHandler() {
    if(this.handler == null){
      this.handler = props.getDynamoHandler();
    }
    return this.handler;
  }
}
