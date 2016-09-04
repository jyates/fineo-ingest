package io.fineo.lambda.handle.staged;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestFirehosePropertyBridge {

  @Test
  public void testAllBindings() throws Exception {
    Properties props = new Properties();
    props.setProperty(StagedFirehosePropertyBridge.FIREHOSE_STAGED_ARCHIVE_NAME_KEY, "archive");
    props.setProperty(StagedFirehosePropertyBridge.FIREHOSE_STAGED_MALFORMED_NAME_KEY, "mal");
    props.setProperty(StagedFirehosePropertyBridge.FIREHOSE_STAGED_ERROR_NAME_KEY, "error");

    AllInjectedBindings bindings = Guice.createInjector(
      new PropertiesModule(props),
      new StagedFirehosePropertyBridge().withAllBindings()
    ).getInstance(AllInjectedBindings.class);
    assertEquals("archive", bindings.archive);
    assertEquals("mal", bindings.mal);
    assertEquals("error", bindings.error);
  }

  @Test
  public void testArchiveBinding() throws Exception {
    Properties props = new Properties();
    props.setProperty(StagedFirehosePropertyBridge.FIREHOSE_STAGED_ARCHIVE_NAME_KEY, "archive");

    ArchiveInjectedBinding bindings = Guice.createInjector(
      new PropertiesModule(props),
      new StagedFirehosePropertyBridge().withArchive()
    ).getInstance(ArchiveInjectedBinding.class);
    assertEquals(props.getProperty(StagedFirehosePropertyBridge.FIREHOSE_STAGED_ARCHIVE_NAME_KEY),
      bindings.archive);
  }

  public static class AllInjectedBindings {
    public final String archive, mal, error;

    @Inject
    public AllInjectedBindings(
      @Named(StagedFirehosePropertyBridge.FIREHOSE_STAGED_ARCHIVE_NAME_KEY) String archive,
      @Named(StagedFirehosePropertyBridge.FIREHOSE_STAGED_MALFORMED_NAME_KEY) String mal,
      @Named(StagedFirehosePropertyBridge.FIREHOSE_STAGED_ERROR_NAME_KEY) String error) {
      this.archive = archive;
      this.mal = mal;
      this.error = error;
    }
  }

  public static class ArchiveInjectedBinding {
    public final String archive;

    @Inject
    public ArchiveInjectedBinding(
      @Named(StagedFirehosePropertyBridge.FIREHOSE_STAGED_ARCHIVE_NAME_KEY) String archive) {
      this.archive = archive;
    }
  }
}
