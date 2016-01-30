package io.fineo.lambda;

import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.resources.AwsResourceManager;
import io.fineo.lambda.util.EndToEndTestRunner;
import io.fineo.lambda.util.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

/**
 * Similar to the local TestEndToEndLambda, but leverages actual AWS services to support
 * access
 */
@Category(AwsDependentTests.class)
public class TestEndToEndLambdaAws {

  @ClassRule
  public static AwsCredentialResource awsCredentials = new AwsCredentialResource();

  private LambdaClientProperties props;
  private EndToEndTestRunner runner;
  private AwsResourceManager manager;

  @Before
  public void connect() throws Exception {
    this.props = LambdaClientProperties.load();
    props.setAwsCredentialProviderForTesting(awsCredentials.getProvider());

    this.manager = new AwsResourceManager(awsCredentials);
    this.runner = new EndToEndTestRunner(props, manager);
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
    Map<String, Object> json = LambdaTestUtils.createRecords(1, 1)[0];
    runner.run(json);
    runner.validate();
  }
}