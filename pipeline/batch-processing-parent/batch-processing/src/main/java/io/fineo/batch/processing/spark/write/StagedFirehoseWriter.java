package io.fineo.batch.processing.spark.write;

import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.lambda.avro.FirehoseRecordWriter;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Write the records to the 'staged records' Firehose
 */
public class StagedFirehoseWriter implements
                                  VoidFunction<Iterator<GenericRecord>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(StagedFirehoseWriter.class);
  private final BatchOptions props;
  private transient IFirehoseBatchWriter writer;

  public StagedFirehoseWriter(BatchOptions props) {
    this.props = props;
    this.writer = props.getFirehoseWriter();
  }

  @Override
  public void call(Iterator<GenericRecord> records) throws Exception {
    LOG.debug("Writing records to firehose");
    FirehoseRecordWriter map = FirehoseRecordWriter.create();
    while (records.hasNext()) {
      getHandler().addToBatch(map.write(records.next()));
    }
    if (this.writer != null) {
      LOG.debug("Have some records to write, flushing them");
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
