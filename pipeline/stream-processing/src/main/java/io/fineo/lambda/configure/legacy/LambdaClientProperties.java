package io.fineo.lambda.configure.legacy;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;

/**
 * Simple wrapper around java properties
 */
public class LambdaClientProperties implements Serializable{

  public static final String TEST_PREFIX = "fineo.integration.test.prefix";
  public static final String KINESIS_URL = "fineo.kinesis.url";
  public static final String KINESIS_PARSED_RAW_OUT_STREAM_NAME = "fineo.kinesis.parsed";
  public static final String KINESIS_RETRIES = "fineo.kinesis.retries";

  public static final String DYNAMO_REGION = "fineo.dynamo.region";
  public static final String DYNAMO_URL_FOR_TESTING = "fineo.dynamo.testing.url";
  public static final String DYNAMO_SCHEMA_STORE_TABLE = "fineo.dynamo.schema-store";
  public static final String DYNAMO_INGEST_TABLE_PREFIX = "fineo.dynamo.ingest.prefix";
  public static final String DYNAMO_READ_LIMIT = "fineo.dynamo.limit.read";
  public static final String DYNAMO_WRITE_LIMIT = "fineo.dynamo.limit.write";
  public static final String DYNAMO_RETRIES = "fineo.dynamo.limit.retries";

  public static final String FIREHOSE_URL = "fineo.firehose.url";
  /* These prefixes combine with the StreamType below to generate the full property names */
  public static final String RAW_PREFIX = "fineo.firehose.raw";
  public static final String STAGED_PREFIX = "fineo.firehose.staged";

  private Properties props;

  /**
   * Use the static {@link #load()}. This is only exposed <b>FOR TESTING</b> and injection
   */
  @VisibleForTesting
  public LambdaClientProperties() {
  }

  @Inject(optional = true)
  public void setProps(Properties props) {
    this.props = props;
  }

  public static LambdaClientProperties load() throws IOException {
    return load(PropertiesLoaderUtil.load());
  }

  private static LambdaClientProperties load(Properties props) {
    Injector injector =
      Guice.createInjector(new PropertiesModule(props), instanceModule(props),
        new DefaultCredentialsModule());
    return injector.getInstance(LambdaClientProperties.class);
  }

  public static LambdaClientProperties create(AbstractModule... modules) {
    Injector injector = Guice.createInjector(modules);
    return injector.getInstance(LambdaClientProperties.class);
  }


  @VisibleForTesting
  public String getSchemaStoreTable() {
    return props.getProperty(DYNAMO_SCHEMA_STORE_TABLE);
  }

  public String getKinesisEndpoint() {
    return props.getProperty(KINESIS_URL);
  }

  public String getRawToStagedKinesisStreamName() {
    return props.getProperty(KINESIS_PARSED_RAW_OUT_STREAM_NAME);
  }

  private String getFirehoseUrl() {
    return props.getProperty(FIREHOSE_URL);
  }

  public String getFirehoseStreamName(String phaseName, StreamType type) {
    return props.getProperty(getFirehoseStreamPropertyVisibleForTesting(phaseName, type));
  }

  @VisibleForTesting
  public static String getFirehoseStreamPropertyVisibleForTesting(String phase, StreamType type) {
    return type.getPropertyKey(phase);
  }

  public String getDynamoIngestTablePrefix() {
    return props.getProperty(DYNAMO_INGEST_TABLE_PREFIX);
  }

  @VisibleForTesting
  public String getTestPrefix() {
    return props.getProperty(TEST_PREFIX);
  }

  @VisibleForTesting
  public Properties getRawPropertiesForTesting() {
    return this.props;
  }
}
