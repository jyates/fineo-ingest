package io.fineo.batch.processing.spark.write;

import com.google.inject.Guice;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.firehose.FirehoseFunctions;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.staged.FirehosePropertyBridge;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Write the records to the 'staged records' Firehose
 */
public class StagedFirehoseWriter implements
                                  VoidFunction<Iterator<GenericRecord>>, Serializable {
  private final Properties props;

  public StagedFirehoseWriter(Properties props) {
    this.props = props;
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
    return Guice.createInjector(newArrayList(
      new PropertiesModule(this.props),
      DefaultCredentialsModule.create(this.props),
      new FirehoseModule(), new FirehoseFunctions(), new FirehosePropertyBridge()
    )).getInstance(
      Key.get(FirehoseBatchWriter.class, Names.named(FirehoseModule.FIREHOSE_ARCHIVE_STREAM)));
  }
}
