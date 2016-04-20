package io.fineo.lambda.e2e.resources.aws.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.fineo.lambda.e2e.resources.aws.AwsResource;
import io.fineo.lambda.e2e.resources.kinesis.ClosableSupplier;
import io.fineo.lambda.e2e.resources.kinesis.IKinesisStreams;
import io.fineo.lambda.kinesis.IKinesisProducer;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Manage interactions with Kinesis streams
 */
public class KinesisStreamManager implements AwsResource, IKinesisStreams {
  private static final Log LOG = LogFactory.getLog(KinesisStreamManager.class);
  private final List<String> streams = new ArrayList<>();
  private final int kinesisRetries = 3;

  private final AWSCredentialsProvider credentials;
  private final ResultWaiter.ResultWaiterFactory waiter;
  private final String region;
  private final int shardCount;

  private AmazonKinesisAsyncClient kinesis;
  private ShardIteratorManager shards;

  @Inject
  public KinesisStreamManager(AWSCredentialsProvider provider,
    ResultWaiter.ResultWaiterFactory waiter, @Named("aws.region") String region,
    @Named("aws.kinesis.shard.count") int shardCount) {
    this.credentials = provider;
    this.waiter = waiter;
    this.region = region;
    this.shardCount = shardCount;
  }

  @Override
  public void setup(String streamName) {
    this.kinesis = getKinesis(region);
    streams.add(streamName);

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

    this.shards = new ShardIteratorManager(kinesis);
  }

  private AmazonKinesisAsyncClient getKinesis(String region) {
    AmazonKinesisAsyncClient client = new AmazonKinesisAsyncClient(credentials);
    client.setRegion(RegionUtils.getRegion(region));
    return client;
  }

  @Override
  public IKinesisProducer getProducer() {
    return new KinesisProducer(this.kinesis, kinesisRetries);
  }

  @Override
  public void submit(String streamName, ByteBuffer data) {
    Preconditions.checkNotNull(streamName, "Stream name cannot be null when writing data!");
    PutRecordRequest request =
      new PutRecordRequest().withStreamName(streamName)
                            .withData(data)
                            .withPartitionKey("1");
    PutRecordResult result = this.kinesis.putRecord(request);
    LOG.info("Successfully put record in kinesis [" + streamName + "]: " + result);
  }

  @Override
  public BlockingQueue<List<ByteBuffer>> getEventQueue(String stream) {
    ClosableSupplier<GetRecordsResult> iter = shards.getShardIterator(stream);
    return new IteratorBlockingQueue(iter);
  }

  private class IteratorBlockingQueue extends AbstractQueue<List<ByteBuffer>>
    implements BlockingQueue<List<ByteBuffer>> {

    private static final long WAIT_INTERVAL = 100;
    private static final int MAX_ATTEMPTS = 3;
    private final ClosableSupplier<GetRecordsResult> iter;
    private List<ByteBuffer> next;

    public IteratorBlockingQueue(ClosableSupplier<GetRecordsResult> iterator) {
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
          // remark the interruption
          Thread.currentThread().interrupt();
          return null;
        }
        GetRecordsResult result = iter.get();
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

  @Override
  public void cleanup(FutureWaiter futures) {
    // delete the streams
    streams.stream().forEach(name -> futures.run(() -> {
      DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
      deleteStreamRequest.setStreamName(name);
      kinesis.deleteStream(deleteStreamRequest);

      waiter.get()
            .withDescription("Ensure delete of kinesis stream: " + name)
            .withStatusNull(() -> kinesis.describeStream(name))
            .waitForResult();
    }));
  }

  @Override
  public Iterable<String> getStreamNames() {
    return this.streams;
  }

  public AmazonKinesisAsyncClient getKinesis() {
    return this.kinesis;
  }
}
