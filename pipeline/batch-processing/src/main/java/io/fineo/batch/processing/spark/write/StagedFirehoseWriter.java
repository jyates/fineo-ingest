package io.fineo.batch.processing.spark.write;

import io.fineo.batch.processing.spark.ModuleLoader;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Write the records to the 'staged records' Firehose
 */
public class StagedFirehoseWriter implements
                                  VoidFunction<Iterator<GenericRecord>>, Serializable {
  private final ModuleLoader modules;

  public StagedFirehoseWriter(ModuleLoader modules) {
    this.modules = modules;
  }

  @Override
  public void call(Iterator<GenericRecord> records) throws Exception {
    FirehoseBatchWriter writer = getWriter();
    FirehoseRecordWriter map = FirehoseRecordWriter.create();
    while (records.hasNext()) {
      writer.addToBatch(map.write(records.next()));
    }
    writer.flush();
  }

  public FirehoseBatchWriter getWriter() {
    return modules.load(FirehoseBatchWriter.class);
  }
}
