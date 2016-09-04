package io.fineo.lambda.configure.firehose;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import io.fineo.schema.Pair;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Load various firehoses and default bindings for function transformations
 */
public class FirehoseModule extends AbstractModule implements Serializable {

  public static final String FIREHOSE_ARCHIVE_STREAM = "fineo.firehose.archive";
  public static final String FIREHOSE_MALFORMED_RECORDS_STREAM = "fineo.firehose.error.malformed";
  public static final String FIREHOSE_COMMIT_ERROR_STREAM = "fineo.firehose.error.commit";
  public static final String FIREHOSE_ARCHIVE_FUNCTION = "firehose.archive.function";
  public static final String FIREHOSE_MALFORMED_RECORDS_FUNCTION = "firehose.malformed.function";
  public static final String FIREHOSE_COMMIT_ERROR_FUNCTION = "firehose.commit.function";
  public static final String FIREHOSE_COMMIT_ERROR_STREAM_NAME =
    "fineo.firehose.error.commit.name";
  public static final String FIREHOSE_MALFORMED_RECORDS_STREAM_NAME =
    "fineo.firehose.error.malformed.name";
  public static final String FIREHOSE_ARCHIVE_STREAM_NAME = "fineo.firehose.archive.name";

  private final List<Pair<String, Class<? extends Provider<IFirehoseBatchWriter>>>> bindings =
    new ArrayList<>();
  private boolean first;

  public FirehoseModule() {
    withArchive().withMalformed().withError();
    this.first = true;
  }

  public FirehoseModule withArchive() {
    checkFirst();
    bindings.add(new Pair<>(FIREHOSE_ARCHIVE_STREAM, ArchiveWriter.class));
    return this;
  }

  public FirehoseModule withMalformed() {
    checkFirst();
    bindings.add(new Pair<>(FIREHOSE_MALFORMED_RECORDS_STREAM, MalformedWriter.class));
    return this;
  }

  public FirehoseModule withError() {
    checkFirst();
    bindings.add(new Pair<>(FIREHOSE_COMMIT_ERROR_STREAM, ErrorWriter.class));
    return this;
  }

  private void checkFirst() {
    if (!first) {
      return;
    }
    first = false;
    this.bindings.clear();
  }

  @Override
  protected void configure() {
    for (Pair<String, Class<? extends Provider<IFirehoseBatchWriter>>> bind : bindings) {
      bind(IFirehoseBatchWriter.class)
        .annotatedWith(Names.named(bind.getKey()))
        .toProvider(bind.getValue())
        .in(Singleton.class);
    }
  }

  @Provides
  @Inject
  @Singleton
  public AmazonKinesisFirehoseAsyncClient getFirehoseClient(AWSCredentialsProvider credentials,
    @Named(FineoProperties.FIREHOSE_URL) String url) {
    AmazonKinesisFirehoseAsyncClient client = new AmazonKinesisFirehoseAsyncClient(credentials);
    client.setEndpoint(url);
    return client;
  }

  private static class Writer implements Provider<IFirehoseBatchWriter> {
    private final Function<ByteBuffer, ByteBuffer> transform;
    private final String name;
    private final AmazonKinesisFirehoseAsyncClient client;

    public Writer(Function<ByteBuffer, ByteBuffer> transform, String name,
      AmazonKinesisFirehoseAsyncClient firehoseClient) {
      this.transform = transform;
      this.name = name;
      this.client = firehoseClient;
    }

    @Override
    public IFirehoseBatchWriter get() {
      return new FirehoseBatchWriter(name, client, transform);
    }
  }


  private static class ArchiveWriter extends Writer {
    @Inject
    public ArchiveWriter(
      @Named(FIREHOSE_ARCHIVE_FUNCTION) Function<ByteBuffer, ByteBuffer> transform,
      @Named(FIREHOSE_ARCHIVE_STREAM_NAME) String name,
      AmazonKinesisFirehoseAsyncClient firehoseClient) {
      super(transform, name, firehoseClient);
    }
  }

  private static class MalformedWriter extends Writer {
    @Inject
    public MalformedWriter(
      @Named(FIREHOSE_MALFORMED_RECORDS_FUNCTION) Function<ByteBuffer, ByteBuffer> transform,
      @Named(FIREHOSE_MALFORMED_RECORDS_STREAM_NAME) String name,
      AmazonKinesisFirehoseAsyncClient firehoseClient) {
      super(transform, name, firehoseClient);
    }
  }

  private static class ErrorWriter extends Writer {
    @Inject
    public ErrorWriter(
      @Named(FIREHOSE_COMMIT_ERROR_FUNCTION) Function<ByteBuffer, ByteBuffer> transform,
      @Named(FIREHOSE_COMMIT_ERROR_STREAM_NAME) String name,
      AmazonKinesisFirehoseAsyncClient firehoseClient) {
      super(transform, name, firehoseClient);
    }
  }
}
