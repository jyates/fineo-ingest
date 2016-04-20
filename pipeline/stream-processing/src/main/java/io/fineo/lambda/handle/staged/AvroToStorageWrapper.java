package io.fineo.lambda.handle.staged;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.fineo.lambda.configure.firehose.FirehoseFunctions;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaWrapper;

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
  public AvroToStorageWrapper(Module... modules) {
    super(AvroToStorageHandler.class, modules);
  }

  @VisibleForTesting
  public static Module[] getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    addDynamo(modules);

    //firehose
    modules.add(new FirehoseModule());
    modules.add(new FirehosePropertyBridge());
    modules.add(new FirehoseFunctions());
    return modules.toArray(new Module[0]);
  }

  @Override
  public void handle(KinesisEvent event) throws IOException {
    getInstance().handle(event);
  }
}
