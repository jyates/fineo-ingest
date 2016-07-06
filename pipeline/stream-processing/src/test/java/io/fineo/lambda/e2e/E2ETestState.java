package io.fineo.lambda.e2e;

import io.fineo.lambda.util.ResourceManager;

public class E2ETestState {
  private EndToEndTestRunner runner;
  private ResourceManager resources;

  public E2ETestState(EndToEndTestRunner runner, ResourceManager resources) {
    this.runner = runner;
    this.resources = resources;
  }

  public EndToEndTestRunner getRunner() {
    return runner;
  }

  public ResourceManager getResources() {
    return resources;
  }
}
