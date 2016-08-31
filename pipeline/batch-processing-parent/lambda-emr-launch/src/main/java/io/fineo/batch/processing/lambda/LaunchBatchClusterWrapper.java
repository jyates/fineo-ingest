package io.fineo.batch.processing.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Module;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.util.InstanceToNamed;
import io.fineo.lambda.handle.LambdaWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Wrapper around the the launcher of EMR clusters
 *
 * @see LaunchBatchProcessingEmrCluster
 */
public class LaunchBatchClusterWrapper
  extends LambdaWrapper<Map<String, Object>, LaunchBatchProcessingEmrCluster> {

  public LaunchBatchClusterWrapper() throws IOException {
    this(getModules());
  }

  public LaunchBatchClusterWrapper(List<Module> modules) {
    super(LaunchBatchProcessingEmrCluster.class, modules);
  }


  @Override
  public void handle(Map<String, Object> event, Context context) throws IOException {
    handleInternal(event, context);
  }

  @Override
  public void handleEvent(Map<String, Object> event) throws IOException {
    getInstance().handle(event);
  }

  private static List<Module> getModules() throws IOException {
    return newArrayList(
      PropertiesModule.load(),
      InstanceToNamed.property("aws.region", "us-east-1"),
      new DefaultCredentialsModule(),
      new EmrClientModule()
    );
  }
}
