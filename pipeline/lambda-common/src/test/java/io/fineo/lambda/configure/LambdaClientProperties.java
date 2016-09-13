package io.fineo.lambda.configure;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;

/**
 * Simple wrapper around java properties
 */
public class LambdaClientProperties implements Serializable{

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
    return props.getProperty(FineoProperties.DYNAMO_SCHEMA_STORE_TABLE);
  }

  public String getRawToStagedKinesisStreamName() {
    return props.getProperty(FineoProperties.KINESIS_PARSED_RAW_OUT_STREAM_NAME);
  }

  public String getFirehoseStreamName(String phaseName, StreamType type) {
    return props.getProperty(getFirehoseStreamPropertyVisibleForTesting(phaseName, type));
  }

  @VisibleForTesting
  public static String getFirehoseStreamPropertyVisibleForTesting(String phase, StreamType type) {
    return type.getPropertyKey(phase);
  }

  public String getDynamoIngestTablePrefix() {
    return props.getProperty(FineoProperties.DYNAMO_INGEST_TABLE_PREFIX);
  }

  @VisibleForTesting
  public String getTestPrefix() {
    return props.getProperty(FineoProperties.TEST_PREFIX);
  }

  @VisibleForTesting
  public Properties getRawPropertiesForTesting() {
    return this.props;
  }
}
