package io.fineo.lambda.configure;

import com.google.inject.AbstractModule;

import java.util.Properties;

public class PropertiesModule extends AbstractModule {

  private final Properties props;

  public PropertiesModule(Properties props) {
    this.props = props;
  }

  @Override
  protected void configure() {
    bind(Properties.class).toInstance(props);
  }
}
