package io.fineo.lambda.e2e.resources;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.base.Preconditions;
import io.fineo.lambda.IngestBaseLambda;
import io.fineo.lambda.test.TestableLambda;
import io.fineo.lambda.util.LambdaTestUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
    private boolean local;

    private IngestUtilBuilder() {
    }

    public IngestUtilBuilder start(String stream, Object lambda) throws NoSuchMethodException {
      return then(stream, lambda);
    }

    public IngestUtilBuilder then(String stream, Object lambda, Function<KinesisEvent, ?> caller) {
      get(stages, stream).add(new Lambda(lambda, caller));
      return this;
    }

    public IngestUtilBuilder then(String stream, Object lambda) throws NoSuchMethodException {
      return then(stream, lambda, local ? getMockTestingMethod(lambda) :
                                  new LambdaCaller<>((IngestBaseLambda) lambda));
    }

    public Map<String, List<Lambda>> build() {
      return stages;
    }

    public IngestUtilBuilder local() {
      this.local = true;
      return this;
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

  private static Function<KinesisEvent, ?> getMockTestingMethod(Object lambda)
    throws NoSuchMethodException {
    Preconditions.checkArgument(lambda instanceof TestableLambda);
    Method handler = TestableLambda.getHandler((TestableLambda) lambda);
    return event -> {
      try {
        return handler.invoke(lambda, event);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    };
  }

  private static class LambdaCaller<T> implements Function<KinesisEvent, T> {

    private final IngestBaseLambda lambda;

    private LambdaCaller(IngestBaseLambda lambda) {
      this.lambda = lambda;
    }

    @Override
    public T apply(KinesisEvent event) {
      try {
        lambda.handler(event);
        return null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
