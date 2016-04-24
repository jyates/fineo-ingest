package io.fineo.batch.processing.spark.write;

import com.google.inject.Guice;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.AvroToDynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Write the records to dynamo
 */
public class DynamoWriter implements VoidFunction<Iterator<GenericRecord>>, Serializable {

  private final Properties props;

  public DynamoWriter(Properties props) {
    this.props = props;
  }

  @Override
  public void call(Iterator<GenericRecord> genericRecordIterator) throws Exception {
    RecordToDynamoHandler handler = getHandler();
    handler.handle(genericRecordIterator);
    MultiWriteFailures<GenericRecord> failed = handler.flush();
    if (failed.any()) {
      throw new RuntimeException("Failed to store some records!" + failed.getActions());
    }
  }

  public RecordToDynamoHandler getHandler() {
    return Guice.createInjector(newArrayList(
      new PropertiesModule(this.props),
      DefaultCredentialsModule.create(this.props),
      new DynamoModule(),
      new AvroToDynamoModule(),
      new DynamoRegionConfigurator()
    )).getInstance(RecordToDynamoHandler.class);
  }
}
