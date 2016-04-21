package io.fineo.lambda.e2e;

import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.e2e.validation.ArchiveValidation;
import io.fineo.lambda.e2e.validation.DynamoWrites;
import io.fineo.lambda.e2e.validation.ErrorStreams;
import io.fineo.lambda.e2e.validation.KinesisValidation;
import io.fineo.lambda.e2e.validation.TriFunction;
import io.fineo.lambda.e2e.validation.ValidationStep;
import io.fineo.lambda.util.ResourceManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;

import static io.fineo.lambda.configure.legacy.LambdaClientProperties.*;
import static io.fineo.lambda.configure.legacy.LambdaClientProperties.STAGED_PREFIX;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static io.fineo.lambda.e2e.validation.ValidationUtils.combine;
import static java.util.stream.Collectors.toList;

public class EndToEndTestBuilder {

  private final ResourceManager manager;
  private final LambdaClientProperties props;
  private List<PhaseValidationBuilder> validations = new ArrayList<>();

  public EndToEndTestBuilder(ResourceManager manager, Properties props) {
    this(create(new PropertiesModule(props), instanceModule(props)),
      manager);
  }

  public EndToEndTestBuilder(LambdaClientProperties props, ResourceManager manager) {
    this.props = props;
    this.manager = manager;
  }

  public EndToEndTestBuilder validateAll() {
    return validateRawPhase().all().validateStoragePhase().all();
  }

  public RawPhaseValidation validateRawPhase() {
    RawPhaseValidation phase = new RawPhaseValidation(this);
    this.validations.add(phase);
    return phase;
  }

  public StoragePhaseValidation validateStoragePhase() {
    StoragePhaseValidation phase = new StoragePhaseValidation(this);
    this.validations.add(phase);
    return phase;
  }

  public EndToEndTestRunner build() throws Exception {
    List<ValidationStep> steps =
      (List<ValidationStep>) validations.stream().flatMap(
        validation -> validation.steps.stream()).collect(toList());
    EndToEndValidator validator = new EndToEndValidator(steps);
    return new EndToEndTestRunner(props, manager, validator);
  }

  public static class PhaseValidationBuilder<T> {
    protected final PriorityQueue<ValidationStep> steps = new PriorityQueue<>();
    private final EndToEndTestBuilder builder;
    protected final String phase;
    private final TriFunction<ResourceManager, LambdaClientProperties, ProgressTracker, byte[]>
      archiveFunc;

    public PhaseValidationBuilder(EndToEndTestBuilder builder, String phase,
      TriFunction<ResourceManager, LambdaClientProperties, ProgressTracker, byte[]> archiveFunc) {
      this.builder = builder;
      this.phase = phase;
      this.archiveFunc = archiveFunc;
    }

    public T archive() {
      ValidationStep step = new ArchiveValidation(phase, archiveFunc);
      steps.add(step);
      return (T) this;
    }

    public T errorStreams() {
      ValidationStep step = new ErrorStreams(phase);
      steps.add(step);
      return (T) this;
    }

    public EndToEndTestBuilder done() {
      return builder;
    }
  }

  public static class RawPhaseValidation extends PhaseValidationBuilder<RawPhaseValidation> {

    public RawPhaseValidation(EndToEndTestBuilder builder) {
      super(builder, RAW_PREFIX, (m, p, progress) -> progress.sent);
    }

    public EndToEndTestBuilder all() {
      return archive().kinesis().errorStreams().done();
    }

    private RawPhaseValidation kinesis() {
      KinesisValidation validation = new KinesisValidation(phase);
      steps.add(validation);
      return this;
    }
  }

  public static class StoragePhaseValidation
    extends PhaseValidationBuilder<StoragePhaseValidation> {

    public StoragePhaseValidation(EndToEndTestBuilder builder) {
      super(builder, STAGED_PREFIX, (m, l, p) -> {
        String stream = l.getFirehoseStreamName(STAGED_PREFIX, StreamType.ARCHIVE);
        List<ByteBuffer> buffs = m.getFirehoseWrites(stream);
        for (ByteBuffer buff: buffs){
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
}
