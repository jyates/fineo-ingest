package io.fineo.lambda.handle;

import com.google.inject.Module;
import io.fineo.lambda.configure.dynamo.AvroToDynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;

import java.util.List;

public class StreamLambdaUtils {

  private StreamLambdaUtils() {
  }

  public static void addDynamo(List<Module> modules){
    modules.add(new DynamoModule());
    modules.add(new AvroToDynamoModule());
    modules.add(new DynamoRegionConfigurator());
  }
}
