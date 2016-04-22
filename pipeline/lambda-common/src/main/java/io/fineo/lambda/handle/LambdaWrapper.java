package io.fineo.lambda.handle;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Wrapper class that calls the actual lambda function, instantiating the caller class, as
 * necessary.
 */
public abstract class LambdaWrapper<T, C extends LambdaHandler<?>> {

  private final Class<C> clazz;
  private final List<Module> modules;
  private C inst;

  public LambdaWrapper(Class<C> handlerClass, Module... modules) {
    this.clazz = handlerClass;
    this.modules = newArrayList(modules);
  }

  protected C getInstance(){
    if (inst == null) {
      Injector guice = Guice.createInjector(modules);
      this.inst = guice.getInstance(clazz);
    }
    return this.inst;
  }

  /**
   * Subclasses have to implement this method themselves. Otherwise, AWS Lambda for some reason
   * thinks we are casting the event to a LinkedHashMap. I don't know. Its weird. You shouldn't
   * have to do much in the method beyond {@link #getInstance()} and then
   * {@link LambdaHandler#handle(Object)}
   * @param event AWS Lambda event
   * @throws IOException on failure
   */
  public abstract void handle(T event) throws IOException;

}
