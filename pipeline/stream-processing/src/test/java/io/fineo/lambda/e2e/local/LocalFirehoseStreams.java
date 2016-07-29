package io.fineo.lambda.e2e.local;

import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.lambda.e2e.util.IngestUtil;
import io.fineo.lambda.e2e.util.ResourceUtils;
import io.fineo.lambda.e2e.util.TestProperties;
import io.fineo.lambda.e2e.manager.collector.OutputCollector;
import io.fineo.lambda.e2e.manager.IFirehoseResource;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.Pair;
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
public class LocalFirehoseStreams implements Serializable, IFirehoseResource {

  private final Map<String, List<ByteBuffer>> firehoseWrites = new HashMap<>();
  private transient final Map<String, FirehoseBatchWriter> firehoses = new HashMap<>();
  private LambdaClientProperties props;

  public void setup(LambdaClientProperties props) {
  }

  @Override
  public void init(Injector injector) {
    this.props = injector.getInstance(LambdaClientProperties.class);
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

  @Override
  public void ensureNoDataStored() {
    for (String stream : firehoseWrites.keySet()) {
      Preconditions.checkState(getFirehoseWrites(stream).size() == 0,
        "Have some writes for stream [%s]", stream);
    }
  }

  @Override
  public void clone(List<Pair<String, StreamType>> toClone, OutputCollector dir)
    throws IOException {
    for (Pair<String, StreamType> stream : toClone) {
      String name = props.getFirehoseStreamName(stream.getKey(), stream.getValue());
      ResourceUtils.writeStream(name, dir, () -> this.getFirehoseWrites(name));
    }
  }

  public void cleanup() {
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

  @Override
  public void cleanup(FutureWaiter waiter) {
    for (List<ByteBuffer> firehose : firehoseWrites.values()) {
      firehose.clear();
    }
  }
}
