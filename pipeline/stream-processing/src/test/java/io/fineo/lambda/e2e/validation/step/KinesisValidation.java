package io.fineo.lambda.e2e.validation.step;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.EventFormTracker;
import io.fineo.lambda.e2e.validation.util.ValidationUtils;
import io.fineo.lambda.util.ResourceManager;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class KinesisValidation extends ValidationStep {

  public KinesisValidation(String phase) {
    super(phase);
  }

  @Override
  public void validate(ResourceManager manager, LambdaClientProperties props,
    EventFormTracker progress) throws IOException {
    String stream = props.getRawToStagedKinesisStreamName();
    ValidationUtils.verifyAvroRecordsFromStream(manager, progress, stream,
      () -> manager.getKinesisWrites(stream));
  }
}