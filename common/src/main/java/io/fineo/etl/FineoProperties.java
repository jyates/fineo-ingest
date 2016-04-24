package io.fineo.etl;

/**
 *
 */
public class FineoProperties {
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
}
