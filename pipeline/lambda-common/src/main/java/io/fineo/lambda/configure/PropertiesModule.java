package io.fineo.lambda.configure;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * Simple module that loads the passed properties into the namespace
 */
public class PropertiesModule extends AbstractModule implements Serializable {

  private final Properties props;

  public PropertiesModule() throws IOException {
    this(PropertiesLoaderUtil.load());
  }

  public PropertiesModule(Properties props) {
    this.props = props;
  }

  @Override
  protected void configure() {
    Names.bindProperties(this.binder(), props);
  }

  public static PropertiesModule load() throws IOException {
    return new PropertiesModule(PropertiesLoaderUtil.load());
  }
}
