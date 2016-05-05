package io.fineo.lambda.e2e.validation.step;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.EventFormTracker;
import io.fineo.lambda.util.ResourceManager;

import java.io.IOException;

public abstract class ValidationStep {
  protected final String phase;

  public ValidationStep(String phase) {
    this.phase = phase;
  }

  public abstract void validate(ResourceManager manager, LambdaClientProperties props,
    EventFormTracker progress) throws IOException, InterruptedException;

  public String getPhase() {
    return phase;
  }
}
