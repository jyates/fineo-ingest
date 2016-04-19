package io.fineo.lambda.configure.util;

import com.google.common.base.Preconditions;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoaderUtil {
  private static final String PROP_FILE_NAME = "fineo-lambda.properties";

  public static Properties load() throws IOException {
    return load(PROP_FILE_NAME);
  }

  public static Properties load(String file) throws IOException {
    InputStream input = LambdaClientProperties.class.getClassLoader().getResourceAsStream(file);
    Preconditions.checkArgument(input != null, "Could not load properties file: " + file);
    Properties props = new Properties();
    props.load(input);
    return props;
  }
}
