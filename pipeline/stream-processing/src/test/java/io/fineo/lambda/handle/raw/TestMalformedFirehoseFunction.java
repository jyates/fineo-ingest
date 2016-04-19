package io.fineo.lambda.handle.raw;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.generic.GenericRecord;
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
      new FirehoseToMalformedInstanceFunctionModule(),
      namedInstance("fineo.firehose.error.commit.name", "commit"),
      namedInstance("fineo.firehose.archive.name", "archive"),
      namedInstance("fineo.firehose.error.malformed.name", "malformed"));
    Function<ByteBuffer, ByteBuffer> func =
      getFunc(inject, FirehoseModule.FIREHOSE_ARCHIVE_FUNCTION);
    ByteBuffer data = ByteBuffer.wrap(new byte[]{1, 2, 3});
    ByteBuffer out = func.apply(data);
    assertTrue(data != out);
    assertEquals(data, out);

    func = getFunc(inject, FirehoseModule.FIREHOSE_COMMIT_FUNCTION);
    out = func.apply(data);
    assertTrue(data != out);
    assertEquals(data, out);

    Function<ByteBuffer, ByteBuffer> malformed = FirehoseToMalformedInstanceFunctionModule.func;
    func = getFunc(inject, FirehoseModule.FIREHOSE_MALFORMED_RECORDS_FUNCTION);
    ByteBuffer expectedData = malformed.apply(data);
    ByteBuffer actualData = func.apply(out);
    FirehoseRecordReader<GenericRecord> reader = FirehoseRecordReader.create(expectedData);
    GenericRecord expected = reader.next();
    reader = FirehoseRecordReader.create(actualData);
    GenericRecord actual = reader.next();
    assertEquals(expected, actual);
  }

  private Function<ByteBuffer, ByteBuffer> getFunc(Injector inject, String key) {
    return inject.getInstance(Key.get(new TypeLiteral<Function<ByteBuffer, ByteBuffer>>() {
    }, Names.named(key)));
  }
}
