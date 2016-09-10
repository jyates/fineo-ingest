package io.fineo.lambda.handle.raw;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.firehose.FirehoseModule;

/**
 * Bridge between the loaded properties and the generic names we use to create the various firehoses
 */
public class FirehosePropertyBridge extends AbstractModule {

  public static final String RAW_FIREHOSE_ARCHIVE = "fineo.firehose.raw.archive";
  public static final String RAW_FIREHOSE_MALFORMED = "fineo.firehose.raw.malformed";
  public static final String RAW_FIREHOSE_ERROR = "fineo.firehose.raw.error";

  @Override
  protected void configure() {
  }

  @Provides
  @Named(FirehoseModule.FIREHOSE_ARCHIVE_STREAM_NAME)
  public String getFirehoseArchiveName(@Named(RAW_FIREHOSE_ARCHIVE) String name) {
    return name;
  }

  @Provides
  @Named(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM_NAME)
  public String getFirehoseMalformedName(@Named(RAW_FIREHOSE_MALFORMED) String name) {
    return name;
  }

  @Provides
  @Named(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM_NAME)
  public String getFirehoseCommitErrorName(@Named(RAW_FIREHOSE_ERROR) String name) {
    return name;
  }
}
