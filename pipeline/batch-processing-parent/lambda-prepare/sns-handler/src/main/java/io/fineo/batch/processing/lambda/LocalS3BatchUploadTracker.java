package io.fineo.batch.processing.lambda;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.inject.Module;
import io.fineo.batch.processing.dynamo.IngestManifestModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class LocalS3BatchUploadTracker
  extends LambdaWrapper<SNSEvent, SnsLocalS3FileEventHandler> {

  public LocalS3BatchUploadTracker() throws IOException {
    super(SnsLocalS3FileEventHandler.class, getModules());
  }

  private static List<Module> getModules() throws IOException {
    return getModules(PropertiesLoaderUtil.load());
  }

  private static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    LambdaWrapper.addBasicProperties(modules, props);
    modules.add(new DynamoModule());
    modules.add(new DynamoRegionConfigurator());
    modules.add(new IngestManifestModule());
    return modules;
  }

  @Override
  public void handle(SNSEvent event) throws IOException {
    getInstance().handle(event);
  }
}
