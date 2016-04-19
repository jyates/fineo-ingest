package io.fineo.lambda.handle.staged;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.Module;
import io.fineo.lambda.handle.LambdaWrapper;

/**
 * Wrapper to instantiate the {@link AvroToStorageHandler}
 */
public class AvroToStorageWrapper extends LambdaWrapper<KinesisEvent, AvroToStorageHandler> {
  public AvroToStorageWrapper() {
    super(AvroToStorageHandler.class, getModules());
  }

  private static Module[] getModules() {
    return null;
  }
}
