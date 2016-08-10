package io.fineo.stream.processing.e2e.options;

import com.beust.jcommander.Parameter;

/**
 *
 */
public class SkipValidation {

  @Parameter(names = "--skip-validation")
  public boolean should = false;
}
