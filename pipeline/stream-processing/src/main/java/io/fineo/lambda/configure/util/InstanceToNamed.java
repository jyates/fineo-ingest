package io.fineo.lambda.configure.util;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class InstanceToNamed<T> extends AbstractModule {

  protected final String named;
  protected final T instance;
  protected final Class<T> clazz;

  public InstanceToNamed(String named, T instance, Class<T> clazz) {
    this.named = named;
    this.instance = instance;
    this.clazz = clazz;
  }

  @Override
  protected void configure() {
    bind(clazz).annotatedWith(Names.named(named)).toInstance(instance);
  }

  public static <T> InstanceToNamed<T> namedInstance(String name, T instance) {
    return new InstanceToNamed<>(name, instance, (Class<T>) instance.getClass());
  }
}
