package io.fineo.lambda.handle.staged;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestBridgeStagedFirehoseProperties {

  @Test
  public void test() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(FirehosePropertyBridge
      .STAGED_FIREHOSE_ARCHIVE, "archive-staged");
    properties.setProperty(FirehosePropertyBridge
      .STAGED_FIREHOSE_ERROR, "error-staged");
    properties.setProperty(FirehosePropertyBridge
      .STAGED_FIREHOSE_MALFORMED, "malformed-staged");
    // ignore the raw properties
    properties.setProperty(io.fineo.lambda.handle.raw.FirehosePropertyBridge.RAW_FIREHOSE_ARCHIVE,
      "archive");
    properties
      .setProperty(io.fineo.lambda.handle.raw.FirehosePropertyBridge.RAW_FIREHOSE_ERROR, "error");
    properties.setProperty(io.fineo.lambda.handle.raw.FirehosePropertyBridge.RAW_FIREHOSE_MALFORMED,
      "malformed");

    Injector guice =
      Guice.createInjector(new PropertiesModule(properties), new FirehosePropertyBridge());

    assertEquals("archive-staged", guice.getInstance(
      Key.get(String.class, Names.named(FirehoseModule.FIREHOSE_ARCHIVE_STREAM_NAME))));
    assertEquals("error-staged", guice.getInstance(Key.get(String.class, Names.named(FirehoseModule
      .FIREHOSE_COMMIT_ERROR_STREAM_NAME))));
    assertEquals("malformed-staged", guice.getInstance(
      Key.get(String.class, Names.named(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM_NAME))));
  }
}
