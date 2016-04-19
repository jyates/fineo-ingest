package io.fineo.lambda.configure;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.util.Properties;

/**
 * Simple module that loads the passed properties into the namespace
 */
public class PropertiesModule extends AbstractModule {

  private final Properties props;

  public PropertiesModule(Properties props) {
    this.props = props;
  }

  @Override
  protected void configure() {
    Names.bindProperties(this.binder(), props);
  }
}
