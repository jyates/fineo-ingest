package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.EventFormTracker;
import io.fineo.lambda.e2e.validation.step.ValidationStep;
import io.fineo.lambda.util.IResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class EndToEndValidator {

  private static final Logger LOG = LoggerFactory.getLogger(EndToEndValidator.class);

  private final List<ValidationStep> steps;

  public EndToEndValidator(List<ValidationStep> steps) {
    this.steps = steps;
  }

  public void validate(IResourceManager manager, LambdaClientProperties properties,
    EventFormTracker progress) throws IOException, InterruptedException {
    LOG.info("\n -------- Validating Test Run ------- \n");
    for (ValidationStep step : steps) {
      String stepName = step.getClass().getSimpleName();
      LOG.info(
        "====> Step validation " + step.getPhase() + " -> " + stepName);
      step.validate(manager, properties, progress);
      LOG.info("=> "+stepName+" SUCCESS");
    }
    LOG.info("\n -------- Validation SUCCESS ------- \n");
  }
}
