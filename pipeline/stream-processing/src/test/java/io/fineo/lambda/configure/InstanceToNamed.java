package io.fineo.lambda.configure;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;

public class InstanceToNamed<T> extends AbstractModule {

  private final String named;
  private final T instance;
  private final Class<T> clazz;

  public InstanceToNamed(String named, T instance, Class<T> clazz) {
    this.named = named;
    this.instance = instance;
    this.clazz = clazz;
  }

  @Override
  protected void configure() {
    if (instance == null) {
      bind(clazz).annotatedWith(Names.named(named)).toProvider(Providers.of(null));
    } else {
      bind(clazz).annotatedWith(Names.named(named)).toInstance(instance);
    }

  }

  public static <T> InstanceToNamed<T> namedInstance(String name, T instance) {
    return new InstanceToNamed<>(name, instance, (Class<T>) instance.getClass());
  }
}
