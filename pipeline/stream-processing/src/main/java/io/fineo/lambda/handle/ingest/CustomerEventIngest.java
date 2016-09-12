package io.fineo.lambda.handle.ingest;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Module;
import io.fineo.lambda.configure.KinesisModule;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.handle.LambdaBaseWrapper;
import io.fineo.lambda.handle.LambdaResponseWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Convert API requests into a JSON event and add it to the target kinesis stream
 */
public class CustomerEventIngest extends
                                 LambdaResponseWrapper<CustomerEventRequest,
                                   CustomerEventResponse,
                                   CustomerEventHandler> {

  public CustomerEventIngest() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public CustomerEventIngest(List<Module> modules) {
    super(CustomerEventHandler.class, modules);
  }

  public static List<Module> getModules(Properties props) throws IOException {
    List<Module> modules = new ArrayList<>();
    LambdaBaseWrapper.addBasicProperties(modules, props);
    modules.add(SingleInstanceModule.instanceModule(new ObjectMapper()));
    modules.add(new KinesisModule());
    return modules;
  }

  @Override
  public CustomerEventResponse handle(CustomerEventRequest input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
