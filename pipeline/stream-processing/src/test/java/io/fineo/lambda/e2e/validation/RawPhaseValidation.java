package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.e2e.EndToEndTestBuilder;
import io.fineo.lambda.e2e.validation.step.KinesisValidation;

import static io.fineo.lambda.configure.legacy.LambdaClientProperties.RAW_PREFIX;

/**
 *
 */
public class RawPhaseValidation extends PhaseValidationBuilder<RawPhaseValidation> {

  public RawPhaseValidation(EndToEndTestBuilder builder) {
    super(builder, RAW_PREFIX, (m, p, progress) -> progress.getSent());
  }

  public EndToEndTestBuilder all() {
    return archive().kinesis().errorStreams().done();
  }

  private RawPhaseValidation kinesis() {
    KinesisValidation validation = new KinesisValidation(phase);
    steps.add(validation);
    return this;
  }
}
