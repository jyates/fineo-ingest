package io.fineo.lambda.handle.staged;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.firehose.FirehoseModule;

/**
 * Bridge between the loaded properties and the generic names we use to create the various firehoses
 */
public class FirehosePropertyBridge extends AbstractModule {

  public static final String STAGED_FIREHOSE_ARCHIVE = "fineo.firehose.staged.archive";
  public static final String STAGED_FIREHOSE_ERROR = "fineo.firehose.staged.error";
  public static final String STAGED_FIREHOSE_MALFORMED = "fineo.firehose.staged.malformed";

  @Override
  protected void configure() {
  }

  @Provides
  @Named(FirehoseModule.FIREHOSE_ARCHIVE_STREAM_NAME)
  public String getFirehoseArchiveName(@Named(STAGED_FIREHOSE_ARCHIVE) String name) {
    return name;
  }

  @Provides
  @Named(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM_NAME)
  public String getFirehoseMalformedName(@Named(STAGED_FIREHOSE_MALFORMED) String name) {
    return name;
  }

  @Provides
  @Named(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM_NAME)
  public String getFirehoseCommitErrorName(
    @Named(STAGED_FIREHOSE_ERROR) String name) {
    return name;
  }
}
