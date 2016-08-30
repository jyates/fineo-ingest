package io.fineo.batch.processing.lambda.sns.remote;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.lambda.runtime.Context;
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

public class RemoteS3BatchUploadTracker
  extends LambdaWrapper<SNSEvent, SnsRemoteS3FileEventHandler> {

  public RemoteS3BatchUploadTracker() throws IOException {
    this(getModules());
  }

  public RemoteS3BatchUploadTracker(List<Module> modules){
    super(SnsRemoteS3FileEventHandler.class, modules);
  }

  private static List<Module> getModules() throws IOException {
    return getModules(PropertiesLoaderUtil.load());
  }

  private static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    LambdaWrapper.addBasicProperties(modules, props);
    modules.add(new DynamoModule());
    modules.add(new DynamoRegionConfigurator());
    modules.add(IngestManifestModule.create(props));
    return modules;
  }


  @Override
  public void handle(SNSEvent event, Context context) throws IOException {
    handleInternal(event, context);
  }

  @Override
  public void handleEvent(SNSEvent event) throws IOException {
    getInstance().handle(event);
  }
}
