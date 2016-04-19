package io.fineo.lambda.handle.raw;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;

/**
 * Bridge between the loaded properties and the generic names we use to create the various firehoses
 */
public class FirehosePropertyBridge extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  @Named("fineo.firehose.archive.name")
  public String getFirehoseArchiveName(@Named("fineo.firehose.raw.archive") String name) {
    return name;
  }

  @Provides
  @Named("fineo.firehose.error.malformed.name")
  public String getFirehoseMalformedName(@Named("fineo.firehose.raw.error") String name) {
    return name;
  }

  @Provides
  @Named("fineo.firehose.error.commit.name")
  public String getFirehoseCommitErrorName(@Named("fineo.firehose.raw.error.commit") String name) {
    return name;
  }
}
