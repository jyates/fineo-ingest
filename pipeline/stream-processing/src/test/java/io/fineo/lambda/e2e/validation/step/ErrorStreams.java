package io.fineo.lambda.e2e.validation.step;

import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.lambda.configure.StreamType;
import io.fineo.lambda.e2e.state.EventFormTracker;
import io.fineo.lambda.e2e.validation.util.ValidationUtils;
import io.fineo.lambda.util.IResourceManager;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

import static io.fineo.lambda.configure.StreamType.COMMIT_ERROR;
import static io.fineo.lambda.configure.StreamType.PROCESSING_ERROR;

public class ErrorStreams extends ValidationStep {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorStreams.class);

  public ErrorStreams(String stage) {
    super(stage);
  }

  @Override
  public void validate(IResourceManager manager, LambdaClientProperties props,
    EventFormTracker progress) {
    LOG.info("Checking to make sure that there are no errors in stage: " + phase);
    verifyNoFirehoseWrites(manager, props, bbs -> {
      return SchemaUtil.toString(LambdaTestUtils.readRecords(ValidationUtils.combine(bbs)));
    }, phase, PROCESSING_ERROR, COMMIT_ERROR);
  }

  private void verifyNoFirehoseWrites(IResourceManager manager, LambdaClientProperties props,
    Function<List<ByteBuffer>, String> errorResult, String stage,
    StreamType... streams) {
    for (StreamType stream : streams) {
      LOG.debug("Checking stream: " + stage + "-" + stream + " has no writes...");
      ValidationUtils
        .empty(errorResult, manager.getFirehoseWrites(props.getFirehoseStreamName(stage, stream)));
      LOG.debug("Stream: " + stage + "-" + stream + " correct");
    }
  }
}
