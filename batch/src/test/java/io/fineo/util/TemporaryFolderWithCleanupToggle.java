package io.fineo.util;

import org.junit.rules.TemporaryFolder;

/**
 * A Temporary folder that can be toggled to optionally not cleanup the temporary file, in the
 * case of debugging tests
 */
public class TemporaryFolderWithCleanupToggle extends TemporaryFolder {

  private boolean cleanup = true;

  public TemporaryFolderWithCleanupToggle() {
    super();
  }

  public TemporaryFolderWithCleanupToggle(boolean cleanup) {
    cleanup = cleanup;
  }

  @Override
  protected void after() {
    if (cleanup) {
      delete();
    }
  }
}
