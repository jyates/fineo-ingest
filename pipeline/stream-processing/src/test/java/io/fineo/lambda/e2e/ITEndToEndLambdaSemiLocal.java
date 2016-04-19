package io.fineo.lambda.e2e;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Module;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.lambda.LocalLambdaRemoteKinesisConnector;
import io.fineo.lambda.handle.raw.RawStageWrapper;
import io.fineo.lambda.handle.staged.AvroToStorageWrapper;
import io.fineo.lambda.util.LambdaTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.handle.util.HandlerUtils.getHandler;

/**
 * Runs the lambda functions themselves locally, but all the other AWS services are run against
 * AWS itself
 */
@Category(AwsDependentTests.class)
public class ITEndToEndLambdaSemiLocal extends BaseITEndToEndAwsServices {
  private static final Log LOG = LogFactory.getLog(ITEndToEndLambdaSemiLocal.class);

  public ITEndToEndLambdaSemiLocal() {
    super(true);
  }

  @Test(timeout = 800000)
  public void testEndToEndSuccess() throws Exception {
    String uuid = "integration-test-" + System.currentTimeMillis() + "-";
    setProperties(getProperties(uuid));

    // setup the stages
    Properties props = getProps().getRawPropertiesForTesting();
    List<Module> modules = updateCredentials(newArrayList(RawStageWrapper.getModules(props)));
    RawStageWrapper start = new RawStageWrapper(modules.toArray(new Module[0]));
    modules = updateCredentials(newArrayList(AvroToStorageWrapper.getModules(props)));
    AvroToStorageWrapper storage = new AvroToStorageWrapper(modules.toArray(new Module[0]));

    // create the connections between stages
    String kinesisIngest = uuid + "ingest";
    String kinesisConnector = getProps().getRawToStagedKinesisStreamName();
    Map<String, List<IngestUtil.Lambda>> mapping =
      IngestUtil.newBuilder()
                .then(kinesisIngest, start, getHandler(start))
                .then(kinesisConnector, storage, getHandler(storage))
                .build();
    LambdaKinesisConnector connector = new LocalLambdaRemoteKinesisConnector();
    connector.configure(mapping, kinesisIngest);
    run(connector, LambdaTestUtils.createRecords(1, 1));
  }

  /**
   * Remove the default credentials and use the ones provided by the test class
   */
  private List<Module> updateCredentials(List<Module> modules) {
    List<Module> ret = new ArrayList<>(modules.size() - 1);
    for (Module module : modules) {
      if (module instanceof DefaultCredentialsModule) {
        continue;
      }
      ret.add(module);
    }
    ret
      .add(new SingleInstanceModule<>(awsCredentials.getProvider(), AWSCredentialsProvider.class));

    return ret;
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
