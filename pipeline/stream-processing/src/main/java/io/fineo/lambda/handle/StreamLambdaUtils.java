package io.fineo.lambda.handle;

import com.google.inject.Module;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.AvroToDynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;

import java.util.List;
import java.util.Properties;

public class StreamLambdaUtils {

  private StreamLambdaUtils() {
  }

  public static void addBasicProperties(List<Module> modules, Properties props) {
    modules.add(new PropertiesModule(props));
    modules.add(new DefaultCredentialsModule());
  }

  public static void addDynamo(List<Module> modules){
    modules.add(new DynamoModule());
    modules.add(new AvroToDynamoModule());
    modules.add(new DynamoRegionConfigurator());
  }
}
