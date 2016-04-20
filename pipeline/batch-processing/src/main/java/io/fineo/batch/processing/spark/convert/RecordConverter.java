package io.fineo.batch.processing.spark.convert;

import com.google.inject.Guice;
import com.google.inject.Module;
import io.fineo.batch.processing.spark.ModuleLoader;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.lambda.kinesis.IKinesisProducer;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Convert raw json events into avro typed records. This makes a large amount of database calls,
 * so you should probably checkpoint the RDD after complete to ensure we don't do it multiple times.
 */
public class RecordConverter implements Function<Map<String, Object>, GenericRecord>, Serializable {

  private final ModuleLoader modules;
  private transient RawJsonToRecordHandler handler;
  private LocalQueueKinesisProducer queue;

  public RecordConverter(ModuleLoader modules) {
    this.modules = modules;
  }

  @Override
  public GenericRecord call(Map<String, Object> json)
    throws Exception {
    RawJsonToRecordHandler handler = getHandler();
    handler.handle(json);
    return queue.getRecords().remove();
  }

  private RawJsonToRecordHandler getHandler() {
    if (this.handler == null) {
      this.queue = new LocalQueueKinesisProducer();
      List<Module> modules = newArrayList(this.modules.getModules(RawJsonToRecordHandler.class));
      modules.add(new SingleInstanceModule<>(queue, IKinesisProducer.class));
      handler = Guice.createInjector(modules).getInstance(RawJsonToRecordHandler.class);
    }
    return handler;
  }
}
