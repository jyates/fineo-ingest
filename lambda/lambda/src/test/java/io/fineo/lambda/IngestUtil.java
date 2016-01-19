package io.fineo.lambda;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.base.Preconditions;
import io.fineo.lambda.avro.FirehoseBatchWriter;
import io.fineo.lambda.avro.KinesisProducer;
import io.fineo.lambda.storage.TestableLambda;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper utility to simulate the ingest path.
 */
public class IngestUtil {

  private final SchemaStore store;
  private final Map<String, List<Lambda>> stages;

  public static class IngestUtilBuilder {

    private final SchemaStore store;
    private Map<String, List<Lambda>> stages = new HashMap<>();

    private IngestUtilBuilder(SchemaStore store) {
      this.store = store;
    }

    public IngestUtilBuilder start(Object lambda) throws NoSuchMethodException {
      Preconditions.checkArgument(lambda instanceof TestableLambda);
      return start(lambda, TestableLambda.getHandler((TestableLambda) lambda));
    }

    public IngestUtilBuilder start(Object lambda, Method handler) {
      List<Lambda> starts = EndToEndTestUtil.get(stages, null);
      starts.add(new Lambda(null, lambda, handler));
      return this;
    }

    public IngestUtilBuilder then(String stream, Object lambda) throws NoSuchMethodException {
      Preconditions.checkArgument(lambda instanceof TestableLambda);
      return then(stream, lambda, TestableLambda.getHandler((TestableLambda) lambda));
    }

    public IngestUtilBuilder then(String stream, Object lamdba, Method handler) {
      EndToEndTestUtil.get(stages, stream).add(new Lambda(stream, lamdba, handler));
      return this;
    }

    public IngestUtil build() {
      return new IngestUtil(stages, store);
    }
  }

  private static class Lambda {
    String stream;
    Object lambda;
    Method handler;

    public Lambda(String stream, Object lambda, Method handler) {
      this.stream = stream;
      this.lambda = lambda;
      this.handler = handler;
    }
  }

  public static IngestUtilBuilder builder() {
    return builder(new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY)));
  }

  public static IngestUtilBuilder builder(SchemaStore store) {
    return new IngestUtilBuilder(store);
  }

  private IngestUtil(Map<String, List<Lambda>> stages, SchemaStore store) {
    this.store = store;
    this.stages = stages;
  }

  public void send(Map<String, Object> json) throws Exception {
    // setup the 'stream' handling
    Map<String, List<ByteBuffer>> events = new HashMap<>();
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);
    Mockito.doAnswer(invocation ->
    {
      String stream = (String) invocation.getArguments()[0];
      GenericRecord record = (GenericRecord) invocation.getArguments()[2];
      ByteBuffer datum = FirehoseRecordWriter.create().write(record);
      List<ByteBuffer> data = EndToEndTestUtil.get(events, stream);
      data.add(datum);
      return null;
    }).when(producer).add(Mockito.anyString(), Mockito.anyString(), Mockito.any());

    // setup the stages to send to the specified listener
    for (List<Lambda> stages : this.stages.values()) {
      for (Lambda stage : stages) {
        Object inst = stage.lambda;
        if (inst instanceof StreamProducer) {
          ((StreamProducer) inst).setDownstreamForTesting(producer);
        }
      }
    }

    // add the first event to the map
    LambdaTestUtils.updateSchemaStore(store, json);
    ByteBuffer start = LambdaTestUtils.asBytes(json);
    EndToEndTestUtil.get(events, null).add(start);

    while (events.size() > 0) {
      for (String key : events.keySet()) {
        List<ByteBuffer> dataEvents = events.get(key);
        List<Lambda> listeners = stages.get(key);
        // send the message to this stage
        for (ByteBuffer message : dataEvents) {
          KinesisEvent event = LambdaTestUtils.getKinesisEvent(message);
          for (Lambda stage : listeners) {
            stage.handler.invoke(stage.lambda, event);
          }
        }
        events.remove(key);
      }
    }
  }

}
