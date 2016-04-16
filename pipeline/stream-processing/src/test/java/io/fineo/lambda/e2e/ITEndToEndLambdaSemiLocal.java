package io.fineo.lambda.e2e;

import com.google.common.collect.Lists;
import io.fineo.aws.AwsDependentTests;
import io.fineo.lambda.LambdaAvroToStorage;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.LambdaRawRecordToAvro;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.lambda.LambdaKinesisConnector;
import io.fineo.lambda.e2e.resources.lambda.LocalLambdaRemoteKinesisConnector;
import io.fineo.lambda.util.LambdaTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class ITEndToEndLambdaSemiLocal extends BaseITEndToEndAwsServices {
  private static final Log LOG = LogFactory.getLog(ITEndToEndLambdaSemiLocal.class);

  public ITEndToEndLambdaSemiLocal(){
    super(true);
  }

  @Test(timeout = 800000)
  public void testEndToEndSuccess() throws Exception {
    String uuid = "integration-test-" + System.currentTimeMillis() + "-";
    setProperties(getProperties(uuid));

    String kinesisIngest = uuid + "ingest";
    String kinesisConnector = getProps().getRawToStagedKinesisStreamName();
    Map<String, List<IngestUtil.Lambda>> mapping =
      IngestUtil.newBuilder()
                .start(kinesisIngest,
                  new LambdaRawRecordToAvro().setPropertiesForTesting(getProps()))
                .then(kinesisConnector,
                  new LambdaAvroToStorage().setPropertiesForTesting(getProps()))
                .build();
    LambdaKinesisConnector connector =
      new LocalLambdaRemoteKinesisConnector(mapping, kinesisIngest);
    run(connector, LambdaTestUtils.createRecords(1, 1));
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