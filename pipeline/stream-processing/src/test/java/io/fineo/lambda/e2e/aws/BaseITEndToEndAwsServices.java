package io.fineo.lambda.e2e.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.EndToEndTestBuilder;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.e2e.resources.aws.firehose.FirehoseStreams;
import io.fineo.lambda.e2e.resources.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.aws.manager.AwsResourceManager;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static java.util.Arrays.asList;

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
  protected final boolean cleanup;

  protected LambdaClientProperties props;
  protected EndToEndTestRunner runner;
  protected AwsResourceManager manager;

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
      if (this.manager != null) {
        this.manager.cleanup(null);
      }
    }
  }

  protected void run(LambdaKinesisConnector connector, Map<String, Object>... msgs)
    throws Exception {
    this.manager = new AwsResourceManager(getCredentialsModule(), output, connector, region,
      getAdditionalModules());
    this.manager.cleanupResourcesOnFailure(cleanup);
    this.runner = new EndToEndTestBuilder(props, manager).validateAll().build();
    runner.setup();

    for (Map<String, Object> json : msgs) {
      runner.run(json);
    }
    Thread.currentThread().sleep(3000);
    runner.validate();
  }

  protected List<Module> getAdditionalModules() {
    return asList(instanceModule(
      new FirehoseStreams(2 * TestProperties.ONE_MINUTE, "s3",
        TestProperties.Firehose.S3_BUCKET_NAME)));
  }

  protected Properties setProperties(String uuid) throws IOException {
    return setProperties(getProperties(uuid));
  }

  protected Properties setProperties(Properties properties) {
    properties.setProperty(LambdaClientProperties.DYNAMO_REGION, region);
    Injector injector = Guice
      .createInjector(new SingleInstanceModule<>(properties), new PropertiesModule(properties));
    this.props = injector.getInstance(LambdaClientProperties.class);
    return properties;
  }

  protected LambdaClientProperties getProps() {
    return props;
  }

  protected Module getCredentialsModule() {
    return new SingleInstanceModule<>(awsCredentials.getProvider(), AWSCredentialsProvider.class);
  }

  private Properties getProperties(String uuid) throws IOException {
    Properties props = new Properties();
    props.setProperty("integration.test.prefix", uuid);
    // fill in test properties
    props.setProperty("kinesis.url", "kinesis.us-east-1.amazonaws.com");
    props.setProperty("kinesis.parsed", uuid + "fineo-parsed-records");
    props.setProperty("kinesis.retries", "3");

    String errorFirehose = "failed-records";
    props.setProperty("firehose.url", "https://firehose.us-east-1.amazonaws.com");
    props.setProperty("firehose.raw.archive", uuid + "fineo-raw-archive");
    props.setProperty("firehose.raw.error", uuid + errorFirehose);
    props.setProperty("firehose.raw.error.commit", uuid + errorFirehose);
    props.setProperty("firehose.staged.archive", uuid + "fineo-staged-archive");
    props.setProperty("firehose.staged.error", uuid + errorFirehose);
    props.setProperty("firehose.staged.error.commit", uuid + errorFirehose);

    props.setProperty("dynamo.region", "us-east-1");
    props.setProperty("dynamo.schema-store", uuid + "schema-customer");
    props.setProperty("dynamo.ingest.prefix", uuid + "customer-ingest");
    props.setProperty("dynamo.limit.write", "1");
    props.setProperty("dynamo.limit.read", "1");
    props.setProperty("dynamo.limit.retries", "3");

    props.setProperty("aws.testing.creds", "aws-testing");

    // replace all the properties with one that is prefixed by "fineo"
    List<String> names = newArrayList(props.stringPropertyNames());
    for (String name : names) {
      String value = props.getProperty(name);
      props.remove(name);
      props.setProperty("fineo." + name, value);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Using properties: ");
      props.store(System.out, "");
    }

    return props;
  }
}