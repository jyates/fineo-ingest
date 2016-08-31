package io.fineo.lambda.configure.util;

import com.google.inject.AbstractModule;

import java.io.Serializable;

public class SingleInstanceModule<T> extends AbstractModule implements Serializable {

  protected final Class<T> clazz;
  protected final T inst;

  public SingleInstanceModule(T inst) {
    this(inst, (Class<T>) inst.getClass());
  }

  public SingleInstanceModule(T inst, Class<T> clazz) {
    this.inst = inst;
    this.clazz = clazz;
  }

  @Override
  protected void configure() {
    bind(clazz).toInstance(inst);
  }

  public static <T> SingleInstanceModule<T> instanceModule(T instance) {
    return new SingleInstanceModule<>(instance);
  }

  public Class<T> getClazz() {
    return clazz;
  }
}
