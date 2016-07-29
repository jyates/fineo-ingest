package io.fineo.lambda.e2e.validation.step;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.EventFormTracker;
import io.fineo.lambda.util.IResourceManager;
import io.fineo.schema.avro.RecordMetadata;

public class DynamoWrites extends ValidationStep {

  public DynamoWrites(String phase) {
    super(phase);
  }

  @Override
  public void validate(IResourceManager manager, LambdaClientProperties props,
    EventFormTracker progress) {
    // verify that we wrote the right things to DynamoDB
    RecordMetadata metadata = RecordMetadata.get(progress.getAvro());
    manager.verifyDynamoWrites(metadata, progress.getJson());
  }
}
