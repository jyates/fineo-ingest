package io.fineo.lambda.handle;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.lambda.handle.LambdaHandler;

import java.io.IOException;

/**
 * Wrapper class that calls the actual lambda function, instantiating the caller class, as
 * necessary.
 */
public class LambdaWrapper<T, C extends LambdaHandler<T>> {

  private final Class<C> clazz;
  private final Module[] modules;
  private C inst;

  public LambdaWrapper(Class<C> handlerClass, Module... modules) {
    this.clazz = handlerClass;
    this.modules = modules;
  }

  public void handle(T event) throws IOException {
    if (inst == null) {
      Injector guice = Guice.createInjector(modules);
      this.inst = guice.getInstance(clazz);
    }
    inst.handle(event);
  }
}
