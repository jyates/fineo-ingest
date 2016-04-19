package io.fineo.lambda.handle;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.lambda.JsonParser;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;

/**
 * Wrapper class that calls the actual lambda function, instantiating the caller class, as
 * necessary.
 */
public class LambdaWrapper<T, C extends LambdaHandler<T>> {

  private final Class<C> clazz;
  private final List<Module> modules;
  private C inst;

  public LambdaWrapper(Class<C> handlerClass, Module... modules) {
    this.clazz = handlerClass;
    this.modules = newArrayList(modules);
  }

  public void handle(T event) throws IOException {
    if (inst == null) {
      Injector guice = Guice.createInjector(modules);
      this.inst = guice.getInstance(clazz);
    }
    inst.handle(event);
  }

  protected static void addBasicProperties(List<Module> modules, Properties props) {
    modules.add(new PropertiesModule(props));
    modules.add(new DefaultCredentialsModule());
    modules.add(instanceModule(new JsonParser()));
  }

  protected static void addDynamo(List<Module> modules){
    modules.add(new DynamoModule());
    modules.add(new DynamoRegionConfigurator());
  }
}
