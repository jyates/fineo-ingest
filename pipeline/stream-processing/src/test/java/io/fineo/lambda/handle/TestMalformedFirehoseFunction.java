package io.fineo.lambda.handle;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.fineo.lambda.configure.firehose.FirehoseFunctions;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static io.fineo.lambda.configure.util.InstanceToNamed.namedInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMalformedFirehoseFunction {

  @Test
  public void testMalformedFunctions() throws Exception {
    Injector inject = Guice.createInjector(
      new FirehoseFunctions(),
      namedInstance("fineo.firehose.error.commit.name", "commit"),
      namedInstance("fineo.firehose.archive.name", "archive"),
      namedInstance("fineo.firehose.error.malformed.name", "malformed"));
    Function<ByteBuffer, ByteBuffer> func =
      getFunc(inject, FirehoseModule.FIREHOSE_ARCHIVE_FUNCTION);
    ByteBuffer data = ByteBuffer.wrap(new byte[]{1, 2, 3});
    ByteBuffer out = func.apply(data);
    assertTrue(data != out);
    assertEquals(data, out);

    assertIsEqualsForFunc(FirehoseModule.FIREHOSE_COMMIT_ERROR_FUNCTION, data, inject);
    assertIsEqualsForFunc(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_FUNCTION, data, inject);
  }

  private void assertIsEqualsForFunc(String name, ByteBuffer data, Injector inject){
    Function<ByteBuffer, ByteBuffer> func = getFunc(inject, name);
    ByteBuffer out = func.apply(data);
    assertTrue(data == out);
    assertEquals(data, out);
  }

  private Function<ByteBuffer, ByteBuffer> getFunc(Injector inject, String key) {
    return inject.getInstance(Key.get(new TypeLiteral<Function<ByteBuffer, ByteBuffer>>() {
    }, Names.named(key)));
  }
}
