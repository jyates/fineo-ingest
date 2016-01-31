package io.fineo.lambda.e2e;

import com.google.common.collect.Lists;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.LambdaClientProperties;
import io.fineo.lambda.e2e.resources.manager.AwsResourceManager;
import io.fineo.lambda.util.LambdaTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Runs the lambda functions themselves locally, but all the other AWS services are run against
 * AWS itself
 */
@Category(AwsDependentTests.class)
public class TestEndToEndLambdaSemiLocal {
  private static final Log LOG = LogFactory.getLog(TestEndToEndLambdaSemiLocal.class);

  @ClassRule
  public static AwsCredentialResource awsCredentials = new AwsCredentialResource();
  @ClassRule
  public static TestOutput output = new TestOutput(false);

  private LambdaClientProperties props;
  private EndToEndTestRunner runner;
  private AwsResourceManager manager;

  @Before
  public void connect() throws Exception {
    this.props = new LambdaClientProperties(getProperties());
    props.setAwsCredentialProviderForTesting(awsCredentials.getProvider());

    this.manager = new AwsResourceManager(awsCredentials, output);
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

  private Properties getProperties() throws IOException {
    Properties props = new Properties();
    String uuid = "integration-test-" + System.currentTimeMillis()+"-";
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
    props.setProperty("dynamo.limit.write", "5");
    props.setProperty("dynamo.limit.read", "7");
    props.setProperty("dynamo.limit.retries", "3");

    // replace all the properties with one that is prefixed by "fineo"
    List<String> names = Lists.newArrayList(props.stringPropertyNames());
    for (String name : names) {
      String value = props.getProperty(name);
      props.remove(name);
      props.setProperty("fineo." + name, value);
    }

    LOG.info("Using properties: ");
    props.store(System.out, "");

    return props;
  }
}
