package io.fineo.lambda.storage;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 *
 */
public interface TestableLambda {
  @VisibleForTesting
  void handleEventInternal(KinesisEvent event) throws IOException;

  static Method getHandler(TestableLambda lambda) throws NoSuchMethodException {
    return  lambda.getClass().getDeclaredMethod("handleEventInternal", KinesisEvent.class);
  }
}
