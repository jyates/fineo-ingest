package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.ProgressTracker;
import io.fineo.lambda.util.ResourceManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Arrays.asList;

public abstract class ValidationStep implements Comparable<ValidationStep> {
  protected final String phase;
  private final int intraPhasePriority;

  public ValidationStep(String phase, int intraPhasePriority) {
    this.phase = phase;
    this.intraPhasePriority = intraPhasePriority;
  }

  @Override
  public int compareTo(ValidationStep o) {
    int index = STAGE_VALIDATION_ORDER.indexOf(phase);
    int remoteIndex = STAGE_VALIDATION_ORDER.indexOf(o.phase);
    int compare = Integer.compare(index, remoteIndex);
    if (compare == 0) {
      compare = Integer.compare(intraPhasePriority, o.intraPhasePriority);
    }
    return compare;
  }

  private static final List<String> STAGE_VALIDATION_ORDER =
    asList(LambdaClientProperties.RAW_PREFIX, LambdaClientProperties.STAGED_PREFIX);

  public abstract void validate(ResourceManager manager, LambdaClientProperties props,
    ProgressTracker progress) throws IOException;

  public String getPhase() {
    return phase;
  }
}
