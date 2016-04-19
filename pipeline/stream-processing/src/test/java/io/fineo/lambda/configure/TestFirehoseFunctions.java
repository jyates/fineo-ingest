package io.fineo.lambda.configure;

import com.google.inject.Guice;
import io.fineo.lambda.handle.raw.FirehoseToMalformedInstanceFunctionModule;
import org.junit.Test;

import static io.fineo.lambda.configure.util.InstanceToNamed.namedInstance;

public class TestFirehoseFunctions {

  @Test
  public void testFunctions() throws Exception {
    Guice
      .createInjector(
        namedInstance("fineo.firehose.error.commit.name", "commit"),
        namedInstance("fineo.firehose.archive.name", "archive"),
        namedInstance("fineo.firehose.error.malformed.name", "malformed"));
  }

  @Test
  public void testMalformedFunctions() throws Exception {
    Guice
      .createInjector(
        new FirehoseToMalformedInstanceFunctionModule(),
        namedInstance("fineo.firehose.error.commit.name", "commit"),
        namedInstance("fineo.firehose.archive.name", "archive"),
        namedInstance("fineo.firehose.error.malformed.name", "malformed"));
  }
}
