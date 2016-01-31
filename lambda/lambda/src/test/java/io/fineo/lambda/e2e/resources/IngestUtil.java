package io.fineo.lambda.e2e.resources;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.base.Preconditions;
import io.fineo.lambda.LambdaAvroToStorage;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.LambdaRawRecordToAvro;
import io.fineo.lambda.StreamProducer;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
import io.fineo.lambda.e2e.resources.kinesis.MockKinesisStreams;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.lambda.test.TestableLambda;
import io.fineo.lambda.util.LambdaTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Helper utility to simulate the ingest path.
 */
public class IngestUtil {

  private static final Log LOG = LogFactory.getLog(IngestUtil.class);
  private final Map<String, List<Lambda>> stages;
  private final IKinesisStreams streams;
  private Map<String, List<ByteBuffer>> handledKinesisEvents = new HashMap<>();

  public static class IngestUtilBuilder {

    private final IKinesisStreams streams;
    private Map<String, List<Lambda>> stages = new HashMap<>();

    public IngestUtilBuilder(IKinesisStreams streams) {
      this.streams = streams;
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
      return new IngestUtil(streams, stages);
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

    @Override
    public String toString() {
      return "Lambda{" +
             "stream='" + stream + '\'' +
             ", lambda=" + lambda +
             ", handler=" + handler +
             '}';
    }
  }

  private IngestUtil(IKinesisStreams streams, Map<String, List<Lambda>> stages) {
    this.streams = streams;
    this.stages = stages;
  }

  public byte[] send(Map<String, Object> json) throws Exception {
    KinesisProducer producer = streams.getProducer();
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
    streams.submit(null, ByteBuffer.wrap(start));
    AtomicReference<Exception> failed = new AtomicReference<>();
    streams.events().forEach(event -> {
      String stream = event.getKey();
      List<ByteBuffer> dataEvents = event.getValue();

      // track the handled events so we can read them all back later
      get(handledKinesisEvents, stream).addAll(dataEvents);

      List<Lambda> listeners = stages.get(stream);
      // send the message to this stage
      for (ByteBuffer message : dataEvents) {
        KinesisEvent kevent = LambdaTestUtils.getKinesisEvent(message);
        for (Lambda stage : listeners) {
          try {
            stage.handler.invoke(stage.lambda, kevent);
          } catch (IllegalAccessException | InvocationTargetException e) {
            LOG.error("Failed to submit event: " + event + " to stage: " + stage, e);
            failed.set(e);
            throw new RuntimeException(e);
          }
        }
      }
    });
    return start;
  }

  public List<ByteBuffer> getKinesisStream(String stream) {
    return get(this.handledKinesisEvents, stream);
  }

  public static <T> List<T> get(Map<String, List<T>> map, String key) {
    List<T> list = map.get(key);
    if (list == null) {
      list = new ArrayList<>();
      map.put(key, list);
    }
    return list;
  }
}