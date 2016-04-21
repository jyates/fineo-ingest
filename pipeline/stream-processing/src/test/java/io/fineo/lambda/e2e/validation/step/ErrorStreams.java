package io.fineo.lambda.e2e.validation.step;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.e2e.EventFormTracker;
import io.fineo.lambda.e2e.validation.util.ValidationUtils;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.lambda.util.SchemaUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

import static io.fineo.lambda.configure.legacy.StreamType.COMMIT_ERROR;
import static io.fineo.lambda.configure.legacy.StreamType.PROCESSING_ERROR;

public class ErrorStreams extends ValidationStep {

  private static final Log LOG = LogFactory.getLog(ErrorStreams.class);

  public ErrorStreams(String stage) {
    super(stage);
  }

  @Override
  public void validate(ResourceManager manager, LambdaClientProperties props,
    EventFormTracker progress) {
    LOG.info("Checking to make sure that there are no errors in stage: " + phase);
    verifyNoFirehoseWrites(manager, props, bbs -> {
      try {
        return SchemaUtil.toString(LambdaTestUtils.readRecords(ValidationUtils.combine(bbs)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, phase, PROCESSING_ERROR, COMMIT_ERROR);
  }

  private void verifyNoFirehoseWrites(ResourceManager manager, LambdaClientProperties props,
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
