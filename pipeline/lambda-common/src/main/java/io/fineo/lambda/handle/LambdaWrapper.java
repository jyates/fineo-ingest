package io.fineo.lambda.handle;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Wrapper class that calls the actual lambda function, instantiating the caller class, as
 * necessary.
 */
public abstract class LambdaWrapper<T, C extends LambdaHandler<?>> {

  private static final String AWS_REQUEST_ID = "AWSRequestId";

  private final Class<C> clazz;
  private final List<Module> modules;
  private C inst;
  private Injector guice;

  public LambdaWrapper(Class<C> handlerClass, List<Module> modules) {
    this.clazz = handlerClass;
    this.modules = modules;
  }

  protected C getInstance() {
    if (inst == null) {
      this.guice = Guice.createInjector(modules);
      this.inst = guice.getInstance(clazz);
    }
    return this.inst;
  }

  /**
   * Subclasses have to implement this method themselves, but all you need to call is
   * {@link #handleInternal(Object, Context)}. This allows AWS to get the java generic type as a
   * real argument and then unpack the event into that type
   *
   * @param event AWS Lambda event
   * @throws IOException on failure
   */
  public abstract void handle(T event, Context context) throws IOException;

  protected final void handleInternal(T event, Context context) throws IOException {
    MDC.clear();
    MDC.put(AWS_REQUEST_ID, context.getAwsRequestId());
    handleEvent(event, context);
  }

  public void handleEvent(T event, Context context) throws IOException {
    handleEvent(event);
  }

  public void handleEvent(T event) throws IOException {
    handleEvent(event);
  }

  public static void addBasicProperties(List<Module> modules, Properties props) {
    modules.add(new PropertiesModule(props));
    modules.add(new DefaultCredentialsModule());
  }

  @VisibleForTesting
  public Injector getGuiceForTesting() {
    return guice;
  }
}
