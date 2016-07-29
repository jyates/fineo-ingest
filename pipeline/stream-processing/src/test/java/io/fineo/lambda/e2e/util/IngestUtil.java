package io.fineo.lambda.e2e.util;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import io.fineo.lambda.util.LambdaTestUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Helper utility to simulate the ingest path.
 */
public class IngestUtil {

  public static class IngestUtilBuilder {
    private Map<String, List<Lambda>> stages = new HashMap<>();

    private IngestUtilBuilder() {
    }

    public IngestUtilBuilder then(String stream, Object lambda, Function<KinesisEvent, ?> caller) {
      get(stages, stream).add(new Lambda(lambda, caller));
      return this;
    }

    public Map<String, List<Lambda>> build() {
      return stages;
    }
  }

  public static IngestUtilBuilder newBuilder() {
    return new IngestUtilBuilder();
  }

  public static class Lambda {
    Object lambda;
    Function<KinesisEvent, ?> handler;

    public Lambda(Object lambda, Function<KinesisEvent, ?> handler) {
      this.lambda = lambda;
      this.handler = handler;
    }

    @Override
    public String toString() {
      return "Lambda{" +
             ", lambda=" + lambda +
             ", handler=" + handler +
             '}';
    }

    public void call(List<ByteBuffer> data) {
      KinesisEvent event = LambdaTestUtils.getKinesisEvent(data);
      this.handler.apply(event);
    }

    public Object getFunction() {
      return this.lambda;
    }
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
