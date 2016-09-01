package io.fineo.batch.processing.spark.convert;

import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Map;

/**
 * Convert raw json events into avro typed records. This makes a large amount of database calls,
 * so you should probably checkpoint the RDD after complete to ensure we don't do it multiple times.
 */
public abstract class RecordConverter<T>
  implements Function<T, GenericRecord>, Serializable {

  private final BatchOptions options;
  private transient RawJsonToRecordHandler handler;
  private LocalQueueKinesisProducer queue;

  public RecordConverter(BatchOptions options) {
   this.options = options;
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
      handler = options.getRawJsonToRecordHandler(queue);
    }
    return handler;
  }
}
