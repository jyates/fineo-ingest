package io.fineo.lambda.e2e.resources;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.base.Preconditions;
import io.fineo.lambda.test.TestableLambda;
import io.fineo.lambda.util.LambdaTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper utility to simulate the ingest path.
 */
public class IngestUtil {

  public static class IngestUtilBuilder {
    private Map<String, List<Lambda>> stages = new HashMap<>();

    private IngestUtilBuilder() {
    }

    public IngestUtilBuilder start(String stream, Object lambda) throws NoSuchMethodException {
      then(stream, lambda);
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

    public Map<String, List<Lambda>> build() {
      return stages;
    }
  }

  public static IngestUtilBuilder newBuilder() {
    return new IngestUtilBuilder();
  }

  public static class Lambda {
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

    public void call(List<ByteBuffer> data) {
      try {
        KinesisEvent event = LambdaTestUtils.getKinesisEvent(data);
        this.handler.invoke(lambda, event);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
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
