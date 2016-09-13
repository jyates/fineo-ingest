package io.fineo.lambda.e2e.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.configure.StreamType;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.e2e.aws.dynamo.DelegateAwsDynamoResource;
import io.fineo.lambda.e2e.aws.firehose.DelegateFirehoseResource;
import io.fineo.lambda.e2e.aws.kinesis.KinesisStreamManager;
import io.fineo.lambda.e2e.aws.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.manager.ManagerBuilder;
import io.fineo.lambda.e2e.manager.collector.FileCollector;
import io.fineo.lambda.e2e.state.EndToEndTestBuilder;
import io.fineo.lambda.e2e.state.EndToEndTestRunner;
import io.fineo.lambda.util.IResourceManager;
import io.fineo.test.rule.TestOutput;
import org.junit.After;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;

/**
 * Base class for tests that use the AWS based resources and the {@link EndToEndTestRunner}
 */
public class BaseITEndToEndAwsServices {

  private static final Logger LOG = LoggerFactory.getLogger(BaseITEndToEndAwsServices.class);
  @ClassRule
  public static AwsCredentialResource awsCredentials = new AwsCredentialResource();
  @ClassRule
  public static TestOutput output = new TestOutput(false);

  protected final String region = System.getProperty("aws-region", "us-east-1");
  private final boolean cleanup;

  private LambdaClientProperties props;
  private EndToEndTestRunner runner;
  private IResourceManager manager;

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
    // setup the manager
    ManagerBuilder managerBuilder = new ManagerBuilder();
    managerBuilder.withAwsCredentials(getCredentialsModule());
    managerBuilder.withRegion(region);
    managerBuilder.withConnector(connector);
    managerBuilder.withCollector(new FileCollector(output));
    DelegateAwsDynamoResource.addAwsDynamo(managerBuilder);
    DelegateFirehoseResource.addFirehose(managerBuilder);
    KinesisStreamManager.addKinesis(managerBuilder);
    managerBuilder.withAdditionalModules(getAdditionalModules());
    managerBuilder.withCleanup(cleanup);

    EndToEndTestBuilder builder = new EndToEndTestBuilder(props, managerBuilder);
    setValidationSteps(builder);
    this.runner = builder.build();
    this.manager = runner.getManager();
    runner.setup();

    for (Map<String, Object> json : msgs) {
      runner.run(json);
    }
    Thread.currentThread().sleep(3000);
    runner.validate();
  }

  protected void setValidationSteps(EndToEndTestBuilder builder) {
    builder.validateAll();
  }

  protected List<Module> getAdditionalModules() {
    return asList();
  }

  protected Properties setProperties(String uuid) throws IOException {
    return setProperties(getProperties(uuid));
  }

  protected Properties setProperties(Properties properties) {
    properties.setProperty(FineoProperties.DYNAMO_REGION, region);
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
    props.setProperty(FineoProperties.TEST_PREFIX, uuid);
    // fill in test properties
    props.setProperty(FineoProperties.KINESIS_URL, "kinesis.us-east-1.amazonaws.com");
    props.setProperty(FineoProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME, uuid + "fineo-parsed-records");
    props.setProperty(FineoProperties.KINESIS_RETRIES, "3");

    String errorFirehose = "failed-records";
    props.setProperty(FineoProperties.FIREHOSE_URL, "https://firehose.us-east-1.amazonaws.com");
    props.setProperty(StreamType.ARCHIVE.getPropertyKey(FineoProperties.RAW_PREFIX), uuid + "fineo-raw-archive");
    props.setProperty(StreamType.PROCESSING_ERROR.getPropertyKey(FineoProperties.RAW_PREFIX), uuid + errorFirehose);
    props.setProperty(StreamType.COMMIT_ERROR.getPropertyKey(FineoProperties.RAW_PREFIX), uuid + errorFirehose);
    props.setProperty(StreamType.ARCHIVE.getPropertyKey(FineoProperties.STAGED_PREFIX), uuid + "fineo-staged-archive");
    props.setProperty(StreamType.PROCESSING_ERROR.getPropertyKey(FineoProperties.STAGED_PREFIX), uuid + errorFirehose);
    props.setProperty(StreamType.COMMIT_ERROR.getPropertyKey(FineoProperties.STAGED_PREFIX), uuid + errorFirehose);

    props.setProperty(FineoProperties.DYNAMO_REGION, "us-east-1");
    props.setProperty(FineoProperties.DYNAMO_SCHEMA_STORE_TABLE, uuid + "schema-customer");
    props.setProperty(FineoProperties.DYNAMO_INGEST_TABLE_PREFIX, uuid + "customer-ingest");
    props.setProperty(FineoProperties.DYNAMO_READ_LIMIT, "1");
    props.setProperty(FineoProperties.DYNAMO_WRITE_LIMIT, "1");
    props.setProperty(FineoProperties.DYNAMO_RETRIES, "3");

    props.setProperty("fineo.aws.testing.creds", "aws-testing");

    if (LOG.isInfoEnabled()) {
      LOG.info("Using properties: ");
      props.store(System.out, "");
    }

    return props;
  }
}
