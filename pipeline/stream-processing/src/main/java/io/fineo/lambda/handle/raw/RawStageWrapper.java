package io.fineo.lambda.handle.raw;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.fineo.lambda.JsonParser;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.KinesisModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
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
    this(getModules(PropertiesLoaderUtil.load()));
  }

  @VisibleForTesting
  public RawStageWrapper(Module... modules) {
    super(RawRecordToAvroHandler.class, modules);
  }

  @VisibleForTesting
  public static Module[] getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    // schema store needs dynamo
    modules.add(new SchemaStoreModule());
    addDynamo(modules);
    // writing to kinesis
    modules.add(new KinesisModule());
    // writing to firehoses
    modules.add(new FirehosePropertyBridge());
    modules.add(new FirehoseModule());
    modules.add(new FirehoseToMalformedInstanceFunctionModule());
    return modules.toArray(new Module[0]);
  }
}