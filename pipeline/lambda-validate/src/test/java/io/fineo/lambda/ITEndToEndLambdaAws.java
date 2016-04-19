package io.fineo.lambda;

import io.fineo.aws.ValidateDeployment;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.e2e.BaseITEndToEndAwsServices;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.resources.RemoteLambdaConnector;
import io.fineo.lambda.util.LambdaTestUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    setProperties(LambdaClientProperties.load());

    String source = getProps().getTestPrefix() + "ingest-source";
    String lambdaConnector = getProps().getRawToStagedKinesisStreamName();

    Map<String, List<String>> mapping = new HashMap<>();
    mapping.put(source, asList(TestProperties.Lambda.getRawToAvroArn(region)));
    mapping.put(lambdaConnector, asList(TestProperties.Lambda.getAvroToStoreArn(region)));
    RemoteLambdaConnector connector = new RemoteLambdaConnector(mapping, source, region);

    run(connector, LambdaTestUtils.createRecords(1, 1));
  }
}
