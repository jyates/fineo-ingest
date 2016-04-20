package io.fineo.batch.processing.spark.write;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.fineo.batch.processing.spark.ModuleLoader;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Write the records to dynamo
 */
public class DynamoWriter implements
                          VoidFunction<Iterator<GenericRecord>>, Serializable {

  private final ModuleLoader modules;

  public DynamoWriter(ModuleLoader modules) {
    this.modules = modules;
  }

  @Override
  public void call(Iterator<GenericRecord> genericRecordIterator) throws Exception {
    RecordToDynamoHandler handler = getHandler();
    handler.handle(genericRecordIterator);
    handler.flush();
  }

  public RecordToDynamoHandler getHandler() {
    return modules.load(RecordToDynamoHandler.class);
  }
}
