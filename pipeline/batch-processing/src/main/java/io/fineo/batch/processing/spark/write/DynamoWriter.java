package io.fineo.batch.processing.spark.write;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.SchemaStoreModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

/**
 * Write the records to dynamo
 */
public class DynamoWriter implements
                          VoidFunction<Iterator<GenericRecord>>, Serializable {
  private final Properties props;

  public DynamoWriter(Properties props) {
    this.props = props;
  }

  @Override
  public void call(Iterator<GenericRecord> genericRecordIterator) throws Exception {
    RecordToDynamoHandler handler = getHandler();
    handler.handle(genericRecordIterator);
    handler.flush();
  }

  public RecordToDynamoHandler getHandler() {
    Injector injector = Guice.createInjector(
      new PropertiesModule(props),
      new DefaultCredentialsModule(),
      new SchemaStoreModule(),
      new DynamoModule(),
      new DynamoRegionConfigurator()
    );
    return injector.getInstance(RecordToDynamoHandler.class);
  }
}
