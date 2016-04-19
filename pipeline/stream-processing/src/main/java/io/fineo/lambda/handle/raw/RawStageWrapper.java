package io.fineo.lambda.handle.raw;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.fineo.lambda.JsonParser;
import io.fineo.lambda.configure.AwsBaseComponentModule;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.FirehoseModule;
import io.fineo.lambda.configure.PropertiesLoaderUtil;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.SchemaStoreModule;
import io.fineo.lambda.handle.LambdaWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.fineo.lambda.configure.SingleInstanceModule.instanceModule;

/**
 * Wrapper to instantiate the raw stage
 */
public class RawStageWrapper extends LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> {
  public RawStageWrapper() throws IOException {
    this(PropertiesLoaderUtil.load());
  }

  public RawStageWrapper(Properties props) {
    super(RawRecordToAvroHandler.class, getModules(props));
  }

  private static Module[] getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    modules.add(new PropertiesModule(props));
    modules.add(new SchemaStoreModule());
    modules.add(new DefaultCredentialsModule());
    modules.add(new AwsBaseComponentModule());
    modules.add(new FirehoseModule());
    modules.add(new FirehosePropertyBridge());
    modules.add(
      Modules.override(new FirehoseModule()).with(new FirehoseToMalformedInstanceFunctionModule()));
    modules.add(instanceModule(new JsonParser()));
    return modules.toArray(new Module[0]);
  }
}
