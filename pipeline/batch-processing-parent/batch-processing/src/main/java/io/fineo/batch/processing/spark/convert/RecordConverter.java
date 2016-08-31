package io.fineo.batch.processing.spark.convert;

import com.google.inject.Guice;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.lambda.kinesis.IKinesisProducer;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * Convert raw json events into avro typed records. This makes a large amount of database calls,
 * so you should probably checkpoint the RDD after complete to ensure we don't do it multiple times.
 */
public abstract class RecordConverter<T>
  implements Function<T, GenericRecord>, Serializable {

  private final Properties props;
  private transient RawJsonToRecordHandler handler;
  private LocalQueueKinesisProducer queue;

  public RecordConverter(Properties props) {
    this.props = props;
  }

  @Override
  public GenericRecord call(T obj)
    throws Exception {
    RawJsonToRecordHandler handler = getHandler();
    handler.handle(transform(obj));
    return queue.getRecords().remove();
  }

  protected abstract Map<String, Object> transform(T obj);


  private RawJsonToRecordHandler getHandler() {
    if (this.handler == null) {
      this.queue = new LocalQueueKinesisProducer();
      handler = Guice.createInjector(
        new PropertiesModule(this.props),
        DefaultCredentialsModule.create(this.props),
        new DynamoModule(),
        new DynamoRegionConfigurator(),
        new SchemaStoreModule(),
        new SingleInstanceModule<>(queue, IKinesisProducer.class)
      ).getInstance(RawJsonToRecordHandler.class);
    }
    return handler;
  }
}
