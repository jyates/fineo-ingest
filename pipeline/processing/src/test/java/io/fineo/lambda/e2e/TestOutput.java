package io.fineo.lambda.e2e;

import org.junit.rules.TemporaryFolder;

/**
 * Derivative of {@link TemporaryFolder}, but doesn't delete the output
 */
public class TestOutput extends TemporaryFolder {

  private final boolean shouldDelete;

  public TestOutput(boolean shouldDelete) {
    this.shouldDelete = shouldDelete;
  }

  @Override
  protected void after() {
    if (shouldDelete) {
      delete();
    }
  }
}
