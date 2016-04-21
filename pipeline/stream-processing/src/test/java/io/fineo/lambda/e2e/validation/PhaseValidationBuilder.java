package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.EndToEndTestBuilder;
import io.fineo.lambda.e2e.EventFormTracker;
import io.fineo.lambda.e2e.validation.step.ArchiveValidation;
import io.fineo.lambda.e2e.validation.step.ErrorStreams;
import io.fineo.lambda.e2e.validation.step.ValidationStep;
import io.fineo.lambda.e2e.validation.util.TriFunction;
import io.fineo.lambda.util.ResourceManager;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Each step is executed in the order it is added. This can be very important for some tests
 */
public class PhaseValidationBuilder<T> {
  protected final Queue<ValidationStep> steps = new ArrayDeque<>();
  private final EndToEndTestBuilder builder;
  protected final String phase;
  private final TriFunction<ResourceManager, LambdaClientProperties, EventFormTracker, byte[]>
    archiveFunc;

  public PhaseValidationBuilder(EndToEndTestBuilder builder, String phase,
    TriFunction<ResourceManager, LambdaClientProperties, EventFormTracker, byte[]> archiveFunc) {
    this.builder = builder;
    this.phase = phase;
    this.archiveFunc = archiveFunc;
  }

  public T archive() {
    ValidationStep step = new ArchiveValidation(phase, archiveFunc);
    steps.add(step);
    return (T) this;
  }

  public T errorStreams() {
    ValidationStep step = new ErrorStreams(phase);
    steps.add(step);
    return (T) this;
  }

  public EndToEndTestBuilder done() {
    return builder;
  }

  public Queue<ValidationStep> getSteps() {
    return steps;
  }
}
