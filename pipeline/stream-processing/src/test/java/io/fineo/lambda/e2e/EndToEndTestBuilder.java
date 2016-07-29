package io.fineo.lambda.e2e;

import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.resources.manager.ManagerBuilder;
import io.fineo.lambda.e2e.resources.manager.ResourceManager;
import io.fineo.lambda.e2e.validation.EndToEndValidator;
import io.fineo.lambda.e2e.validation.PhaseValidationBuilder;
import io.fineo.lambda.e2e.validation.RawPhaseValidation;
import io.fineo.lambda.e2e.validation.StoragePhaseValidation;
import io.fineo.lambda.e2e.validation.step.ValidationStep;
import io.fineo.lambda.util.IResourceManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.fineo.lambda.configure.legacy.LambdaClientProperties.*;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static java.util.stream.Collectors.toList;

public class EndToEndTestBuilder {

  private final IResourceManager manager;
  private final LambdaClientProperties props;
  private List<PhaseValidationBuilder> validations = new ArrayList<>();

  public EndToEndTestBuilder(ManagerBuilder manager, Properties props) {
    this(create(new PropertiesModule(props), instanceModule(props)),
      manager);
  }

  public EndToEndTestBuilder(LambdaClientProperties props, ManagerBuilder manager) {
    this.props = props;
    manager.withProps(props);
    this.manager = manager.build();
  }

  public EndToEndTestBuilder validateAll() {
    return validateRawPhase().all().validateStoragePhase().all();
  }

  public RawPhaseValidation validateRawPhase() {
    return validateRawPhase(10);
  }

  public RawPhaseValidation validateRawPhase(int timeoutSeconds) {
    RawPhaseValidation phase = new RawPhaseValidation(this, timeoutSeconds);
    this.validations.add(phase);
    return phase;
  }

  public StoragePhaseValidation validateStoragePhase() {
    StoragePhaseValidation phase = new StoragePhaseValidation(this);
    this.validations.add(phase);
    return phase;
  }

  public EndToEndTestRunner build() throws Exception {
    List<ValidationStep> steps =
      (List<ValidationStep>) validations.stream().flatMap(
        validation -> validation.getSteps().stream()).collect(toList());
    EndToEndValidator validator = new EndToEndValidator(steps);
    return new EndToEndTestRunner(props, manager, validator);
  }

  public <T extends PhaseValidationBuilder<T>> T addPhase(T phase) {
    this.validations.add(phase);
    return phase;
  }
}
