package io.fineo.lambda.e2e.validation.step;

import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.e2e.state.EventFormTracker;
import io.fineo.lambda.util.IResourceManager;

import java.io.IOException;

public abstract class ValidationStep {
  protected final String phase;

  public ValidationStep(String phase) {
    this.phase = phase;
  }

  public abstract void validate(IResourceManager manager, LambdaClientProperties props,
    EventFormTracker progress) throws IOException, InterruptedException;

  public String getPhase() {
    return phase;
  }
}
