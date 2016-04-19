package io.fineo.lambda.handle.staged;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.Module;
import io.fineo.lambda.JsonParser;
import io.fineo.lambda.configure.AwsBaseComponentModule;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.FirehoseModule;
import io.fineo.lambda.configure.PropertiesLoaderUtil;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.handle.LambdaWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.fineo.lambda.configure.SingleInstanceModule.instanceModule;

/**
 * Wrapper to instantiate the {@link AvroToStorageHandler}
 */
public class AvroToStorageWrapper extends LambdaWrapper<KinesisEvent, AvroToStorageHandler> {
  public AvroToStorageWrapper() throws IOException {
    super(AvroToStorageHandler.class, getModules(PropertiesLoaderUtil.load()));
  }

  private static Module[] getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    modules.add(new PropertiesModule(props));
    modules.add(new DefaultCredentialsModule());
    modules.add(new AwsBaseComponentModule());
    modules.add(new FirehoseModule());
    modules.add(new io.fineo.lambda.handle.staged.FirehosePropertyBridge());
    modules.add(new FirehoseModule());
    modules.add(new DynamoModule());
    modules.add(new DynamoRegionConfigurator());
    modules.add(instanceModule(new JsonParser()));
    return modules.toArray(new Module[0]);
  }
}
