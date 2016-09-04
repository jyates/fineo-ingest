package io.fineo.lambda.handle.staged;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.schema.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Bridge between the loaded properties and the generic names we use to create the various
 * firehoses. This is because we only want to specify things like "fineo.firehose.error.name", but
 * we need to bind those to the specific stage property so we can load the correct stream based
 * on the current stage.
 * <p>
 * This bridge supports the 'staged data' lambda stage
 * </p>
 */
public class StagedFirehosePropertyBridge extends AbstractModule {

  public static final String FIREHOSE_STAGED_ARCHIVE_NAME_KEY = "fineo.firehose.staged.archive";
  public static final String FIREHOSE_STAGED_MALFORMED_NAME_KEY = "fineo.firehose.staged.error";
  public static final String FIREHOSE_STAGED_ERROR_NAME_KEY = "fineo.firehose.staged.error.commit";

  private final List<Pair<Class<? extends Provider<String>>, String>> bindings = new ArrayList<>();

  public StagedFirehosePropertyBridge withAllBindings() {
    return withArchive().withError().withMalformed();
  }

  public StagedFirehosePropertyBridge withArchive() {
    this.bindings.add(new Pair<>(ArchiveName.class, FirehoseModule.FIREHOSE_ARCHIVE_STREAM_NAME));
    return this;
  }

  public StagedFirehosePropertyBridge withMalformed() {
    this.bindings
      .add(new Pair<>(MalformedName.class, FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM_NAME));
    return this;
  }

  public StagedFirehosePropertyBridge withError() {
    this.bindings
      .add(new Pair<>(ErrorName.class, FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM_NAME));
    return this;
  }

  @Override
  protected void configure() {
    for (Pair<Class<? extends Provider<String>>, String> bind : bindings) {
      this.bind(String.class).annotatedWith(Names.named(bind.getValue())).toProvider(bind.getKey());
    }
  }

  public static class ArchiveName implements Provider<String> {
    private final String name;

    @Inject
    public ArchiveName(@Named(FIREHOSE_STAGED_ARCHIVE_NAME_KEY) String name) {
      this.name = name;
    }

    @Override
    public String get() {
      return name;
    }
  }

  public static class MalformedName implements Provider<String> {
    private final String name;

    @Inject
    public MalformedName(@Named(FIREHOSE_STAGED_MALFORMED_NAME_KEY) String name) {
      this.name = name;
    }

    @Override
    public String get() {
      return name;
    }
  }

  public static class ErrorName implements Provider<String> {
    private final String name;

    @Inject
    public ErrorName(@Named(FIREHOSE_STAGED_ERROR_NAME_KEY) String name) {
      this.name = name;
    }

    @Override
    public String get() {
      return name;
    }
  }
}
