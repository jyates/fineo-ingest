package io.fineo.lambda.e2e.resources.firehose;

import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.e2e.resources.IngestUtil;
import io.fineo.lambda.e2e.resources.TestProperties;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.util.run.ResultWaiter;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.fineo.etl.FineoProperties.RAW_PREFIX;
import static io.fineo.etl.FineoProperties.STAGED_PREFIX;

/**
 *
 */
public class LocalFirehoseStreams implements Serializable {

  private final Map<String, List<ByteBuffer>> firehoseWrites = new HashMap<>();
  private transient final Map<String, FirehoseBatchWriter> firehoses = new HashMap<>();
  private LambdaClientProperties props;

  public void setup(LambdaClientProperties props) {
    this.props = props;
    populateStreams();
  }

  public FirehoseBatchWriter getWriter(String prefix, StreamType type) {
    return firehoses.get(props.getFirehoseStreamName(prefix, type));
  }

  public List<ByteBuffer> getFirehoseWrites(String streamName) {
    if (streamName == null) {
      return new ArrayList<>(0);
    }
    ResultWaiter.ResultWaiterFactory waiter = new ResultWaiter.ResultWaiterFactory(
      TestProperties.ONE_MINUTE, TestProperties.ONE_SECOND);
    ResultWaiter<List<ByteBuffer>> wait =
      waiter.get()
            .withDescription("Waiting for firehose writes to propagate...")
            .withStatusNull(() -> {
              List<ByteBuffer> writes = this.firehoseWrites.get(streamName);
              return writes != null && writes.size() > 0 ? writes : null;
            }).withDoneWhenNotNull();
    wait.waitForResult();
    return wait.getLastStatus();
  }

  public void cleanup() {
    for (List<ByteBuffer> firehose : firehoseWrites.values()) {
      firehose.clear();
    }
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    populateStreams();
  }

  private void populateStreams() {
    Stream.of(
      props.getFirehoseStreamName(RAW_PREFIX, StreamType.ARCHIVE),
      props.getFirehoseStreamName(RAW_PREFIX, StreamType.COMMIT_ERROR),
      props.getFirehoseStreamName(RAW_PREFIX, StreamType.PROCESSING_ERROR),
      props.getFirehoseStreamName(STAGED_PREFIX, StreamType.ARCHIVE),
      props.getFirehoseStreamName(STAGED_PREFIX, StreamType.COMMIT_ERROR),
      props.getFirehoseStreamName(STAGED_PREFIX, StreamType.PROCESSING_ERROR))
          .forEach(name -> {
            FirehoseBatchWriter firehose = Mockito.mock(FirehoseBatchWriter.class);
            firehoses.put(name, firehose);
            Mockito.doAnswer(invocation -> {
              ByteBuffer buff = (ByteBuffer) invocation.getArguments()[0];
              IngestUtil.get(firehoseWrites, name).add(buff.duplicate());
              return null;
            }).when(firehose).addToBatch(Mockito.any());
          });
  }
}
