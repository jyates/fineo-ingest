package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.ProgressTracker;
import io.fineo.lambda.util.ResourceManager;

import java.nio.ByteBuffer;
import java.util.List;

import static io.fineo.lambda.configure.legacy.StreamType.ARCHIVE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ArchiveValidation extends ValidationStep {

  private final TriFunction<ResourceManager, LambdaClientProperties, ProgressTracker, byte[]> func;

  public ArchiveValidation(String phase,
    TriFunction<ResourceManager, LambdaClientProperties, ProgressTracker, byte[]> dataExtractor) {
    super(phase, 1);
    this.func = dataExtractor;
  }

  @Override
  public void validate(ResourceManager manager, LambdaClientProperties props,
    ProgressTracker progress) {
    List<ByteBuffer> archived = manager.getFirehoseWrites(props.getFirehoseStreamName(phase,
      ARCHIVE));
    assertNotNull("Got a null set of messages from " + phase + "-archive", archived);
    assertTrue("Didn't get any buffers for " + phase + "-archive in s3", archived.size() > 0);
    ByteBuffer data = ValidationUtils.combine(archived);
    assertTrue("Didn't get any data for " + phase + "-archive in s3", data.remaining() > 0);

    byte[] bytes = func.apply(manager, props, progress);
    // ensure the bytes match from the archived/sent
    String expected = new String(bytes);
    String actual = new String(data.array());
    assertArrayEquals("Validating archived vs stored data in " + phase + " phase\n" +
                      "---- Expected (raw) ->\n[" + expected + "]\n" +
                      "---- Actual (archive)  -> \n[" + actual + "]\n...don't match",
      bytes, data.array());
  }
}
