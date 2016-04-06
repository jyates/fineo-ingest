package io.fineo.lambda.e2e.resources.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import io.fineo.lambda.e2e.resources.AwsResource;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;

import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Manage interactions with Kinesis streams
 */
public class KinesisStreamManager implements AwsResource, IKinesisStreams {

  private final Map<String, String> streamToIterator = new HashMap<>(1);
  private final Queue<List<ByteBuffer>> events = new ConcurrentLinkedDeque<>();
  private final int kinesisRetries = 3;

  private final AWSCredentialsProvider credentials;
  private final ResultWaiter.ResultWaiterFactory waiter;
  private final String region;
  private final int shardCount;

  private AmazonKinesisAsyncClient kinesis;

  public KinesisStreamManager(AWSCredentialsProvider provider,
    ResultWaiter.ResultWaiterFactory waiter, String region, int shardCount) {
    this.credentials = provider;
    this.waiter = waiter;
    this.region = region;
    this.shardCount = shardCount;
  }

  @Override
  public void setup(String streamName) {
    this.kinesis = getKinesis(region);
    streamToIterator.put(streamName, null);

    CreateStreamRequest createStreamRequest = new CreateStreamRequest();
    createStreamRequest.setStreamName(streamName);
    createStreamRequest.setShardCount(shardCount);
    kinesis.createStream(createStreamRequest);
    waiter.get()
          .withDescription("Kinesis stream: [" + streamName + "] activation")
          .withStatus(() -> {
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
            describeStreamRequest.setStreamName(streamName);
            DescribeStreamResult describeStreamResponse =
              kinesis.describeStream(describeStreamRequest);
            return describeStreamResponse.getStreamDescription().getStreamStatus();
          }).withStatusCheck(streamStatus -> streamStatus.equals("ACTIVE"))
          .waitForResult();
  }

  private AmazonKinesisAsyncClient getKinesis(String region) {
    AmazonKinesisAsyncClient client = new AmazonKinesisAsyncClient(credentials);
    client.setRegion(RegionUtils.getRegion(region));
    return client;
  }

  @Override
  public BlockingQueue<List<ByteBuffer>> getEventQueue(String stream, boolean start) {
    String iter = start ? createShardIterator(stream) : getShardIterator(stream);
    return new IteratorBlockingQueue(iter);
  }

  private String getShardIterator(String stream) {
    String iter = streamToIterator.get(stream);
    if (iter == null) {
      iter = createShardIterator(stream);
      streamToIterator.put(stream, iter);
    }
    return iter;

  }

  private String createShardIterator(String stream) {
    GetShardIteratorResult shard =
      kinesis.getShardIterator(stream, "0", "TRIM_HORIZON");
    return shard.getShardIterator();
  }

  private class IteratorBlockingQueue extends AbstractQueue<List<ByteBuffer>>
    implements BlockingQueue<List<ByteBuffer>> {

    private static final long WAIT_INTERVAL = 100;
    private static final int MAX_ATTEMPTS = 3;
    private final String iter;
    private List<ByteBuffer> next;

    public IteratorBlockingQueue(String iterator) {
      this.iter = iterator;
    }

    @Override
    public Iterator<List<ByteBuffer>> iterator() {
      return new AbstractIterator<List<ByteBuffer>>() {
        @Override
        protected List<ByteBuffer> computeNext() {
          List<ByteBuffer> next = poll();
          if (next == null) {
            endOfData();
          }
          return next;
        }
      };
    }

    @Override
    public List<ByteBuffer> take() throws InterruptedException {
      List<ByteBuffer> next = poll();
      while (next == null) {
        Thread.currentThread().sleep(WAIT_INTERVAL);
        next = poll();
      }
      return next;
    }

    @Override
    public List<ByteBuffer> poll(long timeout, TimeUnit unit) throws InterruptedException {
      long end = System.currentTimeMillis() + unit.toMillis(timeout);
      List<ByteBuffer> next = poll();
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
    public List<ByteBuffer> poll() {
      List<ByteBuffer> ret = peek();
      next = null;
      return ret;
    }

    @Override
    public List<ByteBuffer> peek() {
      List<ByteBuffer> ret = null;
      for (int i = 0; i < MAX_ATTEMPTS; i++) {
        try {
          Thread.currentThread().sleep(i * WAIT_INTERVAL);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        GetRecordsResult result =
          kinesis.getRecords(new GetRecordsRequest().withShardIterator(iter));
        ret = result.getRecords().stream().map(Record::getData).collect(Collectors.toList());
        if (ret.size() > 0) {
          break;
        }
      }
      next = ret;
      if (next.size() == 0) {
        next = null;
      }
      return next;
    }

    @Override
    public int size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void put(List<ByteBuffer> t) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(List<ByteBuffer> t, long timeout, TimeUnit unit)
      throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super List<ByteBuffer>> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super List<ByteBuffer>> c, int maxElements) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(List<ByteBuffer> t) {
      throw new UnsupportedOperationException();
    }

  }

  public void cleanup() throws InterruptedException {
    FutureWaiter f = new FutureWaiter(
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(this.streamToIterator.size())));
    cleanup(f);
    f.await();
  }

  public void cleanup(FutureWaiter futures) {
    // delete the streams
    streamToIterator.keySet().stream().forEach(name -> futures.run(() -> {
      DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
      deleteStreamRequest.setStreamName(name);
      kinesis.deleteStream(deleteStreamRequest);

      waiter.get()
            .withDescription("Ensure delete of kinesis stream: " + name)
            .withStatusNull(() -> kinesis.describeStream(name))
            .waitForResult();
    }));
  }

  public AmazonKinesisAsyncClient getKinesis() {
    return this.kinesis;
  }

  public KinesisProducer getProducer() {
    return new KinesisProducer(this.kinesis, kinesisRetries);
  }


  public void submit(String streamName, ByteBuffer data) {
    if (streamName == null) {
      this.events.add(Lists.newArrayList(data));
    }
  }
}
