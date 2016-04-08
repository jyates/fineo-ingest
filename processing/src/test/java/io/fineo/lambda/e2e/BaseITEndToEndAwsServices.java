package io.fineo.lambda.e2e;

import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.manager.AwsResourceManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.ClassRule;

import java.util.Map;

/**
 * Base class for tests that use the {@link AwsResourceManager} and the {@link EndToEndTestRunner}
 */
public class BaseITEndToEndAwsServices {

  private static final Log LOG = LogFactory.getLog(BaseITEndToEndAwsServices.class);
  @ClassRule
  public static AwsCredentialResource awsCredentials = new AwsCredentialResource();
  @ClassRule
  public static TestOutput output = new TestOutput(false);

  protected final String region = System.getProperty("aws-region", "us-east-1");
  private final boolean cleanup;

  private LambdaClientProperties props;
  private EndToEndTestRunner runner;
  private AwsResourceManager manager;

  public BaseITEndToEndAwsServices(boolean cleanup) {
    this.cleanup = cleanup;
  }

  @After
  public void cleanup() throws Exception {
    LOG.info("Doing cleanup...");
    if (this.runner != null) {
      LOG.info("runner not null");
      this.runner.cleanup();
    } else {
      LOG.info("trying manager, runner is null");
      this.manager.cleanup(null);
    }
  }

  protected void run(LambdaKinesisConnector connector, Map<String, Object>... msgs)
    throws Exception {
    this.manager = new AwsResourceManager(awsCredentials, output, connector, region);
    this.manager.cleanupResourcesOnFailure(cleanup);
    this.runner = new EndToEndTestRunner(props, manager);

    for (Map<String, Object> json : msgs) {
      runner.run(json);
    }
    Thread.currentThread().sleep(3000);
    runner.validate();
  }

  protected void setProperties(LambdaClientProperties props) throws Exception {
    this.props = props;
    props.setAwsCredentialProviderForTesting(awsCredentials.getProvider());
  }

  protected LambdaClientProperties getProps() {
    return props;
  }
}
