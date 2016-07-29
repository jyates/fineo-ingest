package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.e2e.state.EndToEndTestBuilder;
import io.fineo.lambda.e2e.validation.step.KinesisValidation;

import static io.fineo.etl.FineoProperties.RAW_PREFIX;

/**
 *
 */
public class RawPhaseValidation extends PhaseValidationBuilder<RawPhaseValidation> {

  private final int timeout;

  public RawPhaseValidation(EndToEndTestBuilder builder, int timeoutSeconds) {
    super(builder, RAW_PREFIX, (m, p, progress) -> progress.getSent());
    this.timeout = timeoutSeconds;
  }

  public EndToEndTestBuilder all() {
    return archive().kinesis().errorStreams().done();
  }

  private RawPhaseValidation kinesis() {
    KinesisValidation validation = new KinesisValidation(phase, timeout);
    steps.add(validation);
    return this;
  }
}
