package io.fineo.lambda.util.mock;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.base.Preconditions;
import io.fineo.lambda.StreamProducer;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.lambda.test.TestableLambda;
import io.fineo.lambda.util.LambdaTestUtils;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper utility to simulate the ingest path.
 */
public class IngestUtil {

  private final Map<String, List<Lambda>> stages;
  private Map<String, List<ByteBuffer>> handledKinesisEvents = new HashMap<>();

  public static class IngestUtilBuilder {

    private Map<String, List<Lambda>> stages = new HashMap<>();

    private IngestUtilBuilder() {
    }

    public IngestUtilBuilder start(Object lambda) throws NoSuchMethodException {
      Preconditions.checkArgument(lambda instanceof TestableLambda);
      return start(lambda, TestableLambda.getHandler((TestableLambda) lambda));
    }

    public IngestUtilBuilder start(Object lambda, Method handler) {
      List<Lambda> starts = get(stages, null);
      starts.add(new Lambda(null, lambda, handler));
      return this;
    }

    public IngestUtilBuilder then(String stream, Object lambda) throws NoSuchMethodException {
      Preconditions.checkArgument(lambda instanceof TestableLambda);
      return then(stream, lambda, TestableLambda.getHandler((TestableLambda) lambda));
    }

    public IngestUtilBuilder then(String stream, Object lamdba, Method handler) {
      get(stages, stream).add(new Lambda(stream, lamdba, handler));
      return this;
    }

    public IngestUtil build() {
      return new IngestUtil(stages);
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

  public static IngestUtilBuilder newBuilder() {
    return new IngestUtilBuilder();
  }

  private IngestUtil(Map<String, List<Lambda>> stages) {
    this.stages = stages;
  }

  public byte[] send(Map<String, Object> json) throws Exception {
    Map<String, List<ByteBuffer>> kinesisEvents = new HashMap<>();
    // setup the 'stream' handling
    KinesisProducer producer = Mockito.mock(KinesisProducer.class);
    Mockito.doAnswer(invocation ->
    {
      String stream = (String) invocation.getArguments()[0];
      GenericRecord record = (GenericRecord) invocation.getArguments()[2];
      ByteBuffer datum = FirehoseRecordWriter.create().write(record);
      List<ByteBuffer> data = get(kinesisEvents, stream);
      data.add(datum);
      return null;
    }).when(producer).add(Mockito.anyString(), Mockito.anyString(), Mockito.any());
    // never have a failure to write
    Mockito.when(producer.flush()).thenReturn(new MultiWriteFailures<>(Collections.emptyList()));


    // setup the stages to send to the specified listener
    for (List<Lambda> stages : this.stages.values()) {
      for (Lambda stage : stages) {
        Object inst = stage.lambda;
        if (inst instanceof StreamProducer) {
          ((StreamProducer) inst).setDownstreamForTesting(producer);
        }
      }
    }

    byte[] start = LambdaTestUtils.asBytes(json);
    get(kinesisEvents, null).add(ByteBuffer.wrap(start));

    while (kinesisEvents.size() > 0) {
      for (String key : kinesisEvents.keySet()) {
        List<ByteBuffer> dataEvents = kinesisEvents.get(key);
        List<Lambda> listeners = stages.get(key);
        // send the message to this stage
        for (ByteBuffer message : dataEvents) {
          KinesisEvent event = LambdaTestUtils.getKinesisEvent(message);
          for (Lambda stage : listeners) {
            stage.handler.invoke(stage.lambda, event);
          }
        }
        // remove the processed events from the queue of things to send
        List<ByteBuffer> processed = kinesisEvents.remove(key);
        if (handledKinesisEvents.get(key) == null) {
          handledKinesisEvents.put(key, processed);
        } else {
          handledKinesisEvents.get(key).addAll(processed);
        }
      }
    }
    return start;
  }

  public List<ByteBuffer> getKinesisStream(String stream) {
    return this.handledKinesisEvents.get(stream);
  }

  static <T> List<T> get(Map<String, List<T>> map, String key) {
    List<T> list = map.get(key);
    if (list == null) {
      list = new ArrayList<>();
      map.put(key, list);
    }
    return list;
  }
}