package io.fineo.lambda;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.aws.ValidateDeployment;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.e2e.aws.BaseITEndToEndAwsServices;
import io.fineo.lambda.e2e.util.TestProperties;
import io.fineo.lambda.e2e.aws.firehose.FirehoseStreams;
import io.fineo.lambda.resources.RemoteLambdaConnector;
import io.fineo.lambda.util.LambdaTestUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.fineo.lambda.configure.util.InstanceToNamed.namedInstance;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static java.util.Arrays.asList;

/**
 * Similar to the local TestEndToEndLambda, but leverages actual AWS services to support
 * access
 */
@Category(ValidateDeployment.class)
public class ITEndToEndLambdaAws extends BaseITEndToEndAwsServices {

  public ITEndToEndLambdaAws() {
    super(true);
  }

  @Test(timeout = 800000)
  public void testEndToEndSuccess() throws Exception {
    Properties props = PropertiesLoaderUtil.load();
    setProperties(props);

    String source = getProps().getTestPrefix() + "ingest-source";
    String lambdaConnector = getProps().getRawToStagedKinesisStreamName();

    Map<String, List<String>> mapping = new HashMap<>();
    mapping.put(source, asList(TestProperties.Lambda.getRawToAvroArn(region)));
    mapping.put(lambdaConnector, asList(TestProperties.Lambda.getAvroToStoreArn(region)));

    Injector injector = Guice.createInjector(
      new PropertiesModule(props),
      getCredentialsModule(),
      namedInstance("aws.region", region)
    );

    RemoteLambdaConnector connector = injector.getInstance(RemoteLambdaConnector.class);
    connector.configure(mapping, source);

    run(connector, LambdaTestUtils.createRecords(1, 1));
  }
}
