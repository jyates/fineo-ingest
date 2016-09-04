package io.fineo.lambda.handle.staged;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.fineo.lambda.configure.firehose.FirehoseFunctions;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.StreamLambdaUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Wrapper to instantiate the {@link AvroToStorageHandler}
 */
public class AvroToStorageWrapper extends LambdaWrapper<KinesisEvent, AvroToStorageHandler> {
  public AvroToStorageWrapper() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  @VisibleForTesting
  public AvroToStorageWrapper(List<Module> modules) {
    super(AvroToStorageHandler.class, modules);
  }

  @VisibleForTesting
  public static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    StreamLambdaUtils.addDynamo(modules);

    //firehose
    modules.add(new FirehoseModule());
    modules.add(new StagedFirehosePropertyBridge().withAllBindings());
    modules.add(new FirehoseFunctions());
    return modules;
  }

  @Override
  public void handle(KinesisEvent event, Context context) throws IOException {
    handleInternal(event, context);
  }

  @Override
  public void handleEvent(KinesisEvent event) throws IOException {
    getInstance().handle(event);
  }
}
