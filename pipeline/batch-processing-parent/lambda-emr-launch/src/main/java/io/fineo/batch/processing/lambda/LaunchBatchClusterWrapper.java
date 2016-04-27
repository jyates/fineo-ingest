package io.fineo.batch.processing.lambda;

import com.google.inject.Module;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.handle.LambdaWrapper;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Wrapper around the the launcher of EMR clusters
 *
 * @see LaunchBatchProcessingEmrCluster
 */
public class LaunchBatchClusterWrapper extends LambdaWrapper<Object, LaunchBatchProcessingEmrCluster> {

  public LaunchBatchClusterWrapper() throws IOException {
    this(getModules());
  }

  public LaunchBatchClusterWrapper(Module... modules) {
    super(LaunchBatchProcessingEmrCluster.class, modules);
  }

  @Override
  public void handle(Object event) throws IOException {
    getInstance().handle(event);
  }

  private static Module[] getModules() throws IOException {
    List<Module> modules = newArrayList(
      PropertiesModule.load(),
      new DefaultCredentialsModule(),
      new EmrClientModule()
    );
    return modules.toArray(new Module[0]);
  }
}