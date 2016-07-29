package io.fineo.lambda.e2e.state;

import io.fineo.lambda.util.IResourceManager;

public class E2ETestState {
  private EndToEndTestRunner runner;
  private IResourceManager resources;

  public E2ETestState(EndToEndTestRunner runner) {
    this.runner = runner;
    this.resources = runner.getManager();
  }

  public EndToEndTestRunner getRunner() {
    return runner;
  }

  public IResourceManager getResources() {
    return resources;
  }
}
