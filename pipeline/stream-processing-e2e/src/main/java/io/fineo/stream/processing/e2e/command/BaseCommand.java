package io.fineo.stream.processing.e2e.command;

import com.google.inject.Module;
import io.fineo.stream.processing.e2e.options.JsonArgument;

import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class BaseCommand {
  public abstract void run(List<Module> baseModules, Map<String, Object> event) throws Exception;
}
