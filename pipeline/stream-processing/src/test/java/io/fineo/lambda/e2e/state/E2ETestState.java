package io.fineo.lambda.e2e.state;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.raw.RawRecordToAvroHandler;
import io.fineo.lambda.handle.staged.AvroToStorageHandler;
import io.fineo.lambda.util.IResourceManager;

public class E2ETestState {
  private EndToEndTestRunner runner;
  private IResourceManager resources;
  private LambdaWrapper<?, ?>[] stages;

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

  public void setStages(LambdaWrapper<?, ?> ...stages){
    this.stages = stages;
  }

  public LambdaWrapper<?, ?>[] getStages() {
    return stages;
  }
}
