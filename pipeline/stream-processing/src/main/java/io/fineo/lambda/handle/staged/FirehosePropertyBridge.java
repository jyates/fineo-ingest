package io.fineo.lambda.handle.staged;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.firehose.FirehoseModule;

/**
 * Bridge between the loaded properties and the generic names we use to create the various firehoses
 */
public class FirehosePropertyBridge extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  @Named(FirehoseModule.FIREHOSE_ARCHIVE_STREAM_NAME)
  public String getFirehoseArchiveName(@Named("fineo.firehose.staged.archive") String name) {
    return name;
  }

  @Provides
  @Named(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM_NAME)
  public String getFirehoseMalformedName(@Named("fineo.firehose.staged.error") String name) {
    return name;
  }

  @Provides
  @Named(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM_NAME)
  public String getFirehoseCommitErrorName(
    @Named("fineo.firehose.staged.error.commit") String name) {
    return name;
  }
}
