package io.fineo.lambda.configure;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.util.Properties;

public class PropertiesModule extends AbstractModule {

  private final Properties props;

  public PropertiesModule(Properties props) {
    this.props = props;
  }

  @Override
  protected void configure() {
    bind(Properties.class).toInstance(props);
    Names.bindProperties(this.binder(), props);
  }
}
