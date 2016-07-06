package io.fineo.lambda.e2e.validation.step;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.EventFormTracker;
import io.fineo.lambda.e2e.validation.util.ValidationUtils;
import io.fineo.lambda.util.ResourceManager;

import java.io.IOException;

public class KinesisValidation extends ValidationStep {

  private final int timeout;

  public KinesisValidation(String phase, int timeoutSeconds) {
    super(phase);
    this.timeout = timeoutSeconds;
  }

  @Override
  public void validate(ResourceManager manager, LambdaClientProperties props,
    EventFormTracker progress) throws IOException, InterruptedException {
    String stream = props.getRawToStagedKinesisStreamName();
    ValidationUtils.verifyAvroRecordsFromStream(manager, progress, stream,
      () -> manager.getKinesisWrites(stream), timeout);
  }
}
