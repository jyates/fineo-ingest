package io.fineo.lambda.avro;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fineo.schema.store.SchemaStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Simple wrapper around java properties
 */
public class FirehoseClientProperties {

  private static final String PROP_FILE_NAME = "fineo-lambda.properties";
  private final java.lang.String KINESIS_URL = "fineo.kinesis.url";
  static final String PARSED_STREAM_NAME = "fineo.kinesis.parsed";

  static final String FIREHOSE_URL = "fineo.firehose.url";
  static final java.lang.String FIREHOSE_MALFORMED_STREAM_NAME = "fineo.firehose.malformed";

  private final Properties props;

  @VisibleForTesting
  FirehoseClientProperties(Properties props) {
    this.props = props;
  }

  public static FirehoseClientProperties load() throws IOException {
    return load(PROP_FILE_NAME);
  }

  public static FirehoseClientProperties load(String file) throws IOException {
    InputStream input = FirehoseClientProperties.class.getClassLoader().getResourceAsStream(file);
    Preconditions.checkArgument(input != null, "Could not load properties file: " + input);
    Properties props = new Properties();
    props.load(input);
    return new FirehoseClientProperties(props);
  }

  public SchemaStore createSchemaStore() {
    return null;
  }

  public String getKinesisEndpoint() {
    return props.getProperty(KINESIS_URL);
  }

  public String getParsedStreamName() {
    return props.getProperty(PARSED_STREAM_NAME);
  }


  public String getFirehoseUrl() {
    return props.getProperty(FIREHOSE_URL);
  }

  public String getFirehoseMalformedStreamName() {
    return props.getProperty(FIREHOSE_MALFORMED_STREAM_NAME);
  }
}
