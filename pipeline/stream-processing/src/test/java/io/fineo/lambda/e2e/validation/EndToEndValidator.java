package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.EventFormTracker;
import io.fineo.lambda.e2e.validation.step.ValidationStep;
import io.fineo.lambda.util.ResourceManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

public class EndToEndValidator {

  private static final Log LOG = LogFactory.getLog(EndToEndValidator.class);

  private final List<ValidationStep> steps;

  public EndToEndValidator(List<ValidationStep> steps) {
    this.steps = steps;
  }

  public void validate(ResourceManager manager, LambdaClientProperties properties,
    EventFormTracker progress) throws IOException {
    LOG.info("\n -------- Validating Test Run ------- \n");
    for (ValidationStep step : steps) {
      LOG.info("Running validation " + step.getPhase() + " -> " + step);
      step.validate(manager, properties, progress);
    }
    LOG.info("\n -------- Validation SUCCESS ------- \n");
  }
}