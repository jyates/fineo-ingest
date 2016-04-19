package io.fineo.lambda.configure.firehose;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.firehose.FirehoseBatchWriter;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Load various firehoses and default bindings for function transformations
 */
public class FirehoseModule extends AbstractModule {

  public static final String FIREHOSE_ARCHIVE_STREAM = "fineo.firehose.archive";
  public static final String FIREHOSE_MALFORMED_RECORDS_STREAM = "fineo.firehose.error.malformed";
  public static final String FIREHOSE_COMMIT_ERROR_STREAM = "fineo.firehose.error.commit";
  public static final String FIREHOSE_ARCHIVE_FUNCTION = "firehose.archive.function";
  public static final String FIREHOSE_MALFORMED_RECORDS_FUNCTION = "firehose.malformed.function";
  public static final String FIREHOSE_COMMIT_FUNCTION = "firehose.commit.function";
  public static final String FIREHOSE_COMMIT_ERROR_STREAM_NAME =
    "fineo.firehose.error.commit.name";
  public static final String FIREHOSE_MALFORMED_RECORDS_STREAM_NAME =
    "fineo.firehose.error.malformed.name";
  public static final String FIREHOSE_ARCHIVE_STREAM_NAME = "fineo.firehose.archive.name";

  @Override
  protected void configure() {
  }

  @Provides
  @Inject
  @Singleton
  public AmazonKinesisFirehoseAsyncClient getFirehoseClient(AWSCredentialsProvider credentials,
    @Named(LambdaClientProperties.FIREHOSE_URL) String url) {
    AmazonKinesisFirehoseAsyncClient client = new AmazonKinesisFirehoseAsyncClient(credentials);
    client.setEndpoint(url);
    return client;
  }

  @Provides
  @Named(FIREHOSE_ARCHIVE_STREAM)
  @Inject
  @Singleton
  public FirehoseBatchWriter getArchiveWriter(
    @Named(FIREHOSE_ARCHIVE_FUNCTION) Function<ByteBuffer, ByteBuffer> transform,
    @Named(FIREHOSE_ARCHIVE_STREAM_NAME) String name,
    AmazonKinesisFirehoseAsyncClient firehoseClient) {
    return new FirehoseBatchWriter(name, firehoseClient, transform);
  }

  @Provides
  @Named(FIREHOSE_MALFORMED_RECORDS_STREAM)
  @Inject
  @Singleton
  public FirehoseBatchWriter getProcessingErrorWriter(
    @Named(FIREHOSE_MALFORMED_RECORDS_FUNCTION) Function<ByteBuffer, ByteBuffer> transform,
    @Named(FIREHOSE_MALFORMED_RECORDS_STREAM_NAME) String name,
    AmazonKinesisFirehoseAsyncClient firehoseClient) {
    return new FirehoseBatchWriter(name, firehoseClient, transform);
  }

  @Provides
  @Named(FIREHOSE_COMMIT_ERROR_STREAM)
  @Inject
  @Singleton
  public FirehoseBatchWriter getCommitErrorWriter(
    @Named(FIREHOSE_COMMIT_FUNCTION) Function<ByteBuffer, ByteBuffer> transform,
    @Named(FIREHOSE_COMMIT_ERROR_STREAM_NAME) String name,
    AmazonKinesisFirehoseAsyncClient firehoseClient) {
    return new FirehoseBatchWriter(name, firehoseClient, transform);
  }
}
