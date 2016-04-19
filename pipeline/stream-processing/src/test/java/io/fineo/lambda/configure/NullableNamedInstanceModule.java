package io.fineo.lambda.configure;

import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import io.fineo.lambda.configure.util.InstanceToNamed;

/**
 * Like the {@link InstanceToNamed}, but supports a null instance
 */
public class NullableNamedInstanceModule<T> extends InstanceToNamed<T> {
  public NullableNamedInstanceModule(String named, T instance, Class<T> clazz) {
    super(named, instance, clazz);
  }

  @Override
  protected void configure() {
    if (instance == null) {
      bind(clazz).annotatedWith(Names.named(named)).toProvider(Providers.of(null));
    } else {
      super.configure();
    }
  }
}
