package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.ProgressTracker;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.RecordMetadata;

/**
 *
 */
public class DynamoWrites extends ValidationStep {

  public DynamoWrites(String phase) {
    super(phase, 3);
  }

  @Override
  public void validate(ResourceManager manager, LambdaClientProperties props,
    ProgressTracker progress) {
    // verify that we wrote the right things to DynamoDB
    RecordMetadata metadata = RecordMetadata.get(progress.avro);
    manager.verifyDynamoWrites(metadata, progress.json);
  }
}
