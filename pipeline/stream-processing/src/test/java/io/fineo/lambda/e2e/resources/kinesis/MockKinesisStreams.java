package io.fineo.lambda.e2e.resources.kinesis;

import com.google.common.collect.Lists;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.e2e.resources.WrappingQueue;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.lambda.avro.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * A Mock front to a set of kinesis streams
 */
public class MockKinesisStreams implements IKinesisStreams {

  private Map<String, List<List<ByteBuffer>>> kinesisEvents = new HashMap<>();
  private IKinesisProducer producer = Mockito.mock(KinesisProducer.class);

  public MockKinesisStreams() throws IOException {
    // setup the 'stream' handling
    Mockito.doAnswer(invocation ->
    {
      String stream = (String) invocation.getArguments()[0];
      GenericRecord record = (GenericRecord) invocation.getArguments()[2];
      ByteBuffer datum = FirehoseRecordWriter.create().write(record);
      submit(stream, datum);
      return null;
    }).when(producer).add(Mockito.anyString(), Mockito.anyString(), Mockito.any());
    // never have a failure to write
    Mockito.when(producer.flush()).thenReturn(new MultiWriteFailures<>(Collections.emptyList()));
  }

  @Override
  public IKinesisProducer getProducer() {
    return this.producer;
  }

  @Override
  public void submit(String streamName, ByteBuffer data) {
    List<List<ByteBuffer>> buff = kinesisEvents.get(streamName);
    buff.add(Lists.newArrayList(data));
  }

  @Override
  public BlockingQueue<List<ByteBuffer>> getEventQueue(String stream) {
    List<List<ByteBuffer>> list = kinesisEvents.get(stream);
    return new WrappingQueue<>(list, 0);
  }

  @Override
  public void setup(String stream) {
    List<List<ByteBuffer>> buff = kinesisEvents.get(stream);
    if (buff == null) {
      buff = Collections.synchronizedList(new ArrayList<>());
      kinesisEvents.put(stream, buff);
    }
  }

  @Override
  public Iterable<String> getStreamNames() {
    return kinesisEvents.keySet();
  }

  public void reset() {
    kinesisEvents.clear();
  }

}
