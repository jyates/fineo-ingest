package io.fineo.lambda.e2e.resources.kinesis;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.schema.Pair;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A Mock front to a set of kinesis streams
 */
public class MockKinesisStreams implements IKinesisStreams {

  private Map<String, List<ByteBuffer>> kinesisEvents = new HashMap<>();
  private KinesisProducer producer = Mockito.mock(KinesisProducer.class);

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
  public KinesisProducer getProducer() {
    return this.producer;
  }

  @Override
  public void submit(String streamName, ByteBuffer data) {
    kinesisEvents.put(streamName, Lists.newArrayList(data));
  }

  @Override
  public Stream<Pair<String, List<ByteBuffer>>> events() {
    Iterable<Pair<String, List<ByteBuffer>>> iter = () -> new AbstractIterator<Pair<String,
      List<ByteBuffer>>>() {
      @Override
      protected Pair<String, List<ByteBuffer>> computeNext() {
        if (kinesisEvents.size() == 0) {
          endOfData();
          return null;
        }

        Map.Entry<String, List<ByteBuffer>> entry = kinesisEvents.entrySet().iterator().next();
        kinesisEvents.remove(entry.getKey());
        return new Pair<>(entry.getKey(), entry.getValue());
      }
    };
    return StreamSupport.stream(iter.spliterator(), false);
  }

  @Override
  public List<ByteBuffer> getEvents(String stream, boolean start) {
    return null;
  }
}
