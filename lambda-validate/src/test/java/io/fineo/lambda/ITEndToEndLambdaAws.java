package io.fineo.lambda;

import io.fineo.aws.ValidateDeployment;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.TestOutput;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.manager.AwsResourceManager;
import io.fineo.lambda.resources.RemoteLambdaConnector;
import io.fineo.lambda.util.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Similar to the local TestEndToEndLambda, but leverages actual AWS services to support
 * access
 */
@Category(ValidateDeployment.class)
public class ITEndToEndLambdaAws {

  @ClassRule
  public static AwsCredentialResource awsCredentials = new AwsCredentialResource();
  @ClassRule
  public static TestOutput output = new TestOutput(false);

  private final String region = System.getProperty("aws-region", "us-east-1");

  private LambdaClientProperties props;
  private EndToEndTestRunner runner;
  private AwsResourceManager manager;

  @Before
  public void connect() throws Exception {
    this.props = LambdaClientProperties.load();
    props.setAwsCredentialProviderForTesting(awsCredentials.getProvider());
  }

  @After
  public void cleanup() throws Exception {
    if (this.runner != null) {
      this.runner.cleanup();
    } else {
      this.manager.cleanup(null);
    }
  }

  @Test
  public void testEndToEndSuccess() throws Exception {
    String uuid = "integration-test-" + System.currentTimeMillis() + "-";
    String source = uuid + "ingest-source";
    String lambdaConnector = uuid + "parsed";

    Map<String, List<String>> mapping = new HashMap<>();
    mapping.put(source, asList(TestProperties.Lambda.getRawToAvroArn(region)));
    mapping.put(lambdaConnector, asList(TestProperties.Lambda.getAvroToStoreArn(region)));
    RemoteLambdaConnector connector = new RemoteLambdaConnector(mapping, source, region);
    this.manager = new AwsResourceManager(awsCredentials, output, connector);
    this.runner = new EndToEndTestRunner(props, manager);

    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    runner.run(json);
    runner.validate();
  }
}
