package io.fineo.batch.processing.spark.write;

import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.lambda.avro.FirehoseRecordWriter;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Write the records to the 'staged records' Firehose
 */
public class StagedFirehoseWriter implements
                                  VoidFunction<Iterator<GenericRecord>>, Serializable {
  private final BatchOptions props;
  private transient IFirehoseBatchWriter writer;

  public StagedFirehoseWriter(BatchOptions props) {
    this.props = props;
    this.writer = props.getFirehoseWriter();
  }

  @Override
  public void call(Iterator<GenericRecord> records) throws Exception {
    FirehoseRecordWriter map = FirehoseRecordWriter.create();
    while (records.hasNext()) {
      getHandler().addToBatch(map.write(records.next()));
    }
    if (this.writer != null) {
      writer.flush();
    }
  }

  private IFirehoseBatchWriter getHandler() {
    if (this.writer == null) {
      this.writer = props.getFirehoseWriter();
    }
    return this.writer;
  }
}
