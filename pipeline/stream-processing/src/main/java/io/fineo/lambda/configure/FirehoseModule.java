package io.fineo.lambda.configure;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
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
  public static final String FIREHOSE_MALFORMED_FUNCTION = "firehose.malformed.function";
  public static final String FIREHOSE_COMMIT_FUNCTION = "firehose.commit.function";

  @Override
  protected void configure() {
    bind(Function.class).annotatedWith(Names.named(FIREHOSE_ARCHIVE_FUNCTION)).toInstance(
      Function.identity());
    bind(Function.class).annotatedWith(Names.named(FIREHOSE_MALFORMED_FUNCTION)).toInstance(
      Function.identity());
    bind(Function.class).annotatedWith(Names.named(FIREHOSE_COMMIT_FUNCTION)).toInstance(
      Function.identity());
  }

  @Provides
  @Named(FIREHOSE_ARCHIVE_STREAM)
  @Inject
  @Singleton
  public FirehoseBatchWriter getArchiveWriter(
    @Named(FIREHOSE_ARCHIVE_FUNCTION) Function<ByteBuffer, ByteBuffer> transform,
    @Named("fineo.firehose.archive.name") String name,
    AmazonKinesisFirehoseAsyncClient firehoseClient) {
    return new FirehoseBatchWriter(name, firehoseClient, transform);
  }

  @Provides
  @Named(FIREHOSE_MALFORMED_RECORDS_STREAM)
  @Inject
  @Singleton
  public FirehoseBatchWriter getProcessingErrorWriter(
    @Named(FIREHOSE_MALFORMED_FUNCTION) Function<ByteBuffer, ByteBuffer> transform,
    @Named("fineo.firehose.error.malformed.name") String name,
    AmazonKinesisFirehoseAsyncClient firehoseClient) {
    return new FirehoseBatchWriter(name, firehoseClient, transform);
  }

  @Provides
  @Named(FIREHOSE_COMMIT_ERROR_STREAM)
  @Inject
  @Singleton
  public FirehoseBatchWriter getCommitErrorWriter(
    @Named(FIREHOSE_COMMIT_FUNCTION) Function<ByteBuffer, ByteBuffer> transform,
    @Named("fineo.firehose.error.commit.name") String name,
    AmazonKinesisFirehoseAsyncClient firehoseClient) {
    return new FirehoseBatchWriter(name, firehoseClient, transform);
  }
}
