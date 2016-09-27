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
public class FirehosePropertyBridge extends AbstractModule {

  public static final String STAGED_FIREHOSE_ARCHIVE = "fineo.firehose.staged.archive";
  public static final String STAGED_FIREHOSE_MALFORMED = "fineo.firehose.staged.malformed";
  public static final String STAGED_FIREHOSE_ERROR = "fineo.firehose.staged.error";

  private final List<Pair<Class<? extends Provider<String>>, String>> bindings = new ArrayList<>();

  public FirehosePropertyBridge withAllBindings() {
    return withArchive().withError().withMalformed();
  }

  public FirehosePropertyBridge withArchive() {
    this.bindings.add(new Pair<>(ArchiveName.class, FirehoseModule.FIREHOSE_ARCHIVE_STREAM_NAME));
    return this;
  }

  public FirehosePropertyBridge withMalformed() {
    this.bindings
      .add(new Pair<>(MalformedName.class, FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM_NAME));
    return this;
  }

  public FirehosePropertyBridge withError() {
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
    public ArchiveName(@Named(STAGED_FIREHOSE_ARCHIVE) String name) {
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
    public MalformedName(@Named(STAGED_FIREHOSE_MALFORMED) String name) {
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
    public ErrorName(@Named(STAGED_FIREHOSE_ERROR) String name) {
      this.name = name;
    }

    @Override
    public String get() {
      return name;
    }
  }
}
