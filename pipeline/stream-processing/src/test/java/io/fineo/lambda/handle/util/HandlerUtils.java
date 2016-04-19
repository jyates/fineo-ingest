package io.fineo.lambda.handle.util;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import io.fineo.lambda.handle.LambdaWrapper;

import java.io.IOException;
import java.util.function.Function;

public class HandlerUtils {

  public static Function<KinesisEvent, ?> getHandler(LambdaWrapper<KinesisEvent, ?> lambda) {
    return event -> {
      try {
        lambda.handle(event);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    };
  }
}
