package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.ProgressTracker;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.TestRecordMetadata;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class KinesisValidation extends ValidationStep {

  public KinesisValidation(String phase) {
    super(phase, 3);
  }

  @Override
  public void validate(ResourceManager manager, LambdaClientProperties props,
    ProgressTracker progress) throws IOException {
    String stream = props.getRawToStagedKinesisStreamName();
    verifyAvroRecordsFromStream(manager, progress, stream, () -> manager.getKinesisWrites(stream));
  }

  private void verifyAvroRecordsFromStream(ResourceManager manager, ProgressTracker progress,
    String stream, Supplier<List<ByteBuffer>> bytes)
    throws IOException {
    List<ByteBuffer> parsedBytes = bytes.get();
    // read the parsed avro records
    List<GenericRecord> parsedRecords = LambdaTestUtils.readRecords(
      ValidationUtils.combine(parsedBytes));
    assertEquals("[" + stream + "] Got unexpected number of records: " + parsedRecords, 1,
      parsedRecords.size());
    GenericRecord record = parsedRecords.get(0);

    // org/schema naming
    TestRecordMetadata.verifyRecordMetadataMatchesExpectedNaming(record);
    ValidationUtils.verifyRecordMatchesJson(manager.getStore(), progress.json, record);
    progress.avro = record;
  }
}
