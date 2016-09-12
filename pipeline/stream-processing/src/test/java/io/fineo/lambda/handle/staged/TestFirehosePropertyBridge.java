package io.fineo.lambda.handle.staged;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.PropertiesModule;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestFirehosePropertyBridge {

  @Test
  public void testAllBindings() throws Exception {
    Properties props = new Properties();
    props.setProperty(FirehosePropertyBridge.STAGED_FIREHOSE_ARCHIVE, "archive");
    props.setProperty(FirehosePropertyBridge.STAGED_FIREHOSE_MALFORMED, "mal");
    props.setProperty(FirehosePropertyBridge.STAGED_FIREHOSE_ERROR, "error");

    AllInjectedBindings bindings = Guice.createInjector(
      new PropertiesModule(props),
      new FirehosePropertyBridge().withAllBindings()
    ).getInstance(AllInjectedBindings.class);
    assertEquals("archive", bindings.archive);
    assertEquals("mal", bindings.mal);
    assertEquals("error", bindings.error);
  }

  @Test
  public void testArchiveBinding() throws Exception {
    Properties props = new Properties();
    props.setProperty(FirehosePropertyBridge.STAGED_FIREHOSE_ARCHIVE, "archive");

    ArchiveInjectedBinding bindings = Guice.createInjector(
      new PropertiesModule(props),
      new FirehosePropertyBridge().withArchive()
    ).getInstance(ArchiveInjectedBinding.class);
    assertEquals(props.getProperty(FirehosePropertyBridge.STAGED_FIREHOSE_ARCHIVE),
      bindings.archive);
  }

  public static class AllInjectedBindings {
    public final String archive, mal, error;

    @Inject
    public AllInjectedBindings(
      @Named(FirehosePropertyBridge.STAGED_FIREHOSE_ARCHIVE) String archive,
      @Named(FirehosePropertyBridge.STAGED_FIREHOSE_MALFORMED) String mal,
      @Named(FirehosePropertyBridge.STAGED_FIREHOSE_ERROR) String error) {
      this.archive = archive;
      this.mal = mal;
      this.error = error;
    }
  }

  public static class ArchiveInjectedBinding {
    public final String archive;

    @Inject
    public ArchiveInjectedBinding(
      @Named(FirehosePropertyBridge.STAGED_FIREHOSE_ARCHIVE) String archive) {
      this.archive = archive;
    }
  }
}
