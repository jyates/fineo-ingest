package io.fineo.lambda.handle.raw;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.fineo.lambda.JsonParser;
import io.fineo.lambda.configure.KinesisModule;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.StreamLambdaUtils;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;

/**
 * Wrapper to instantiate the raw stage
 */
public class RawStageWrapper extends LambdaWrapper<KinesisEvent, RawRecordToAvroHandler> {

  public RawStageWrapper() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public RawStageWrapper(List<Module> modules) {
    super(RawRecordToAvroHandler.class, modules);
  }

  @Override
  public void handle(KinesisEvent event) throws IOException {
    getInstance().handle(event);
  }

  @VisibleForTesting
  public static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    modules.add(instanceModule(new JsonParser()));
    // schema store needs dynamo
    modules.add(new SchemaStoreModule());
    StreamLambdaUtils.addDynamo(modules);
    // writing to kinesis
    modules.add(new KinesisModule());
    // writing to firehoses
    modules.add(new FirehosePropertyBridge());
    modules.add(new FirehoseModule());
    modules.add(new FirehoseToMalformedInstanceFunctionModule());
    return modules;
  }
}
