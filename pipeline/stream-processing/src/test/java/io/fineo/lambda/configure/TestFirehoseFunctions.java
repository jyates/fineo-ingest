package io.fineo.lambda.configure;

import com.google.inject.Guice;
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
}
