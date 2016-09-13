package io.fineo.lambda.e2e.validation;

import io.fineo.lambda.configure.StreamType;
import io.fineo.lambda.e2e.state.EndToEndTestBuilder;
import io.fineo.lambda.e2e.validation.step.DynamoWrites;
import io.fineo.lambda.e2e.validation.step.ValidationStep;

import java.nio.ByteBuffer;
import java.util.List;

import static io.fineo.etl.FineoProperties.STAGED_PREFIX;
import static io.fineo.lambda.e2e.validation.util.ValidationUtils.combine;

public class StoragePhaseValidation
  extends PhaseValidationBuilder<StoragePhaseValidation> {

  public StoragePhaseValidation(EndToEndTestBuilder builder) {
    super(builder, STAGED_PREFIX, (m, l, p) -> {
      String stream = l.getFirehoseStreamName(STAGED_PREFIX, StreamType.ARCHIVE);
      List<ByteBuffer> buffs = m.getFirehoseWrites(stream);
      for (ByteBuffer buff : buffs) {
        buff.rewind();
      }
      return combine(buffs).array();
    });
  }

  public StoragePhaseValidation dynamo() {
    ValidationStep step = new DynamoWrites(phase);
    steps.add(step);
    return this;
  }

  public EndToEndTestBuilder all() {
    return archive().errorStreams().dynamo().done();
  }
}
