package io.fineo.lambda.e2e.resources.kinesis;

import com.google.common.collect.Lists;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.kinesis.KinesisProducer;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A Mock front to a set of kinesis streams
 */
public class MockKinesisStreams implements IKinesisStreams {

  private Map<String, List<List<ByteBuffer>>> kinesisEvents = new HashMap<>();
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
    for (List<List<ByteBuffer>> events : kinesisEvents.values()) {
      events.clear();
    }
  }

  private class WrappingQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {

    private static final int WAIT_INTERVAL = 100;
    private final List<T> backingList;
    private final int offset;
    private int index = 0;

    public WrappingQueue(List<T> byteBuffers) {
      this(byteBuffers, byteBuffers.size());
    }

    public WrappingQueue(List<T> byteBuffers, int offset) {
      this.backingList = byteBuffers;
      this.offset = offset;
    }

    @Override
    public Iterator<T> iterator() {
      // iterator that skips past the previously existing elements
      Iterator<T> delegate = backingList.iterator();
      for (int i = 0; i < offset; i++) {
        delegate.next();
      }
      return delegate;
    }

    @Override
    public T take() throws InterruptedException {
      T next = poll();
      while (next == null) {
        Thread.currentThread().sleep(WAIT_INTERVAL);
        next = poll();
      }
      return next;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
      long end = System.currentTimeMillis() + unit.toMillis(timeout);
      T next = poll();
      while (next == null) {
        long now = System.currentTimeMillis();
        if (now > end || now + WAIT_INTERVAL > end) {
          return null;
        }

        Thread.currentThread().sleep(WAIT_INTERVAL);
        next = poll();
      }
      return next;
    }

    @Override
    public T poll() {
      T next = peek();
      if (next != null) {
        index++;
      }
      return next;
    }

    @Override
    public T peek() {
      if (backingList.size() == (index + offset)) {
        return null;
      }
      return backingList.get(index);
    }


    @Override
    public int size() {
      return backingList.size() - offset - index;
    }

    @Override
    public void put(T t) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super T> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(T t) {
      throw new UnsupportedOperationException();
    }
  }
}
