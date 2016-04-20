package io.fineo.lambda.e2e;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.manager.AwsResourceManager;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.ClassRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
    this.manager = new AwsResourceManager(getCredentialsModule(), output, connector, region,
      getAdditionalModules());
    this.manager.cleanupResourcesOnFailure(cleanup);
    this.runner = new EndToEndTestRunner(props, manager);
    runner.setup();

    for (Map<String, Object> json : msgs) {
      runner.run(json);
    }
    Thread.currentThread().sleep(3000);
    runner.validate();
  }

  protected List<Module> getAdditionalModules() {
    return Collections.emptyList();
  }

  protected void setProperties(Properties properties) {
    properties.setProperty(LambdaClientProperties.DYNAMO_REGION, region);
    Injector injector = Guice
      .createInjector(new SingleInstanceModule<>(properties), new PropertiesModule(properties));
    this.props = injector.getInstance(LambdaClientProperties.class);
  }

  protected LambdaClientProperties getProps() {
    return props;
  }

  protected Module getCredentialsModule() {
    return new SingleInstanceModule<>(awsCredentials.getProvider(), AWSCredentialsProvider.class);
  }
}
