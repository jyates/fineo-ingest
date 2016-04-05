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
import io.fineo.lambda.e2e.resources.ResourceUtils;
import io.fineo.lambda.kinesis.KinesisProducer;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.lambda.util.run.ResultWaiter;
import io.fineo.schema.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Manage interactions with Kinesis streams
 */
public class KinesisStreamManager implements AwsResource {

  private final AWSCredentialsProvider credentials;
  private final ResultWaiter.ResultWaiterFactory waiter;
  private Map<String, String> streamToIterator = new HashMap<>(1);
  private AmazonKinesisAsyncClient kinesis;
  private int kinesisRetries = 3;
  private Queue<List<ByteBuffer>> events = new ConcurrentLinkedDeque<>();

  public KinesisStreamManager(AWSCredentialsProvider provider,
    ResultWaiter.ResultWaiterFactory waiter) {
    this.credentials = provider;
    this.waiter = waiter;
  }

  public void setup(String region, String streamName, int shardCount) {
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

  public List<ByteBuffer> getEvents(String stream, boolean start) {
    if (!start) {
      String iter = streamToIterator.get(stream);
      if (iter == null) {
        iter = createShardIterator(stream);
        streamToIterator.put(stream, iter);
      }
      GetRecordsResult getResult =
        kinesis.getRecords(new GetRecordsRequest().withShardIterator(iter));
      return getResult.getRecords().stream().map(Record::getData).collect(Collectors.toList());
    }
    String iter = createShardIterator(stream);
    GetRecordsResult getResult =
      kinesis.getRecords(new GetRecordsRequest().withShardIterator(iter));
    List<ByteBuffer> events = new ArrayList<>();
    while (getResult.getRecords().size() > 0) {
      for (Record r : getResult.getRecords()) {
        events.add(r.getData());
      }
      getResult = kinesis.getRecords(new GetRecordsRequest().withShardIterator(iter));
    }
    return events;
  }

  private String createShardIterator(String stream) {
    GetShardIteratorResult shard =
      kinesis.getShardIterator(stream, "0", "TRIM_HORIZON");
    return shard.getShardIterator();
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

  public void cloneStream(String streamName, File dir) throws IOException {
    ResourceUtils.writeStream(streamName, dir, () -> this.getEvents(streamName, true));
  }


  public KinesisProducer getProducer() {
    return new KinesisProducer(this.kinesis, kinesisRetries);
  }


  public void submit(String streamName, ByteBuffer data) {
    if (streamName == null) {
      this.events.add(Lists.newArrayList(data));
    }
  }


  public Stream<Pair<String, List<ByteBuffer>>> events() {
    Iterable<Pair<String, List<ByteBuffer>>> iter = () -> new AbstractIterator<Pair<String,
      List<ByteBuffer>>>() {
      @Override
      protected Pair<String, List<ByteBuffer>> computeNext() {
        if (events.size() > 0) {
          return new Pair<>(null, events.remove());
        }

        for (String name : streamToIterator.keySet()) {
          List<ByteBuffer> next = getEvents(name, false);
          if (next.size() > 0) {
            return new Pair<>(name, next);
          }
        }
        endOfData();
        return null;
      }
    };
    return StreamSupport.stream(iter.spliterator(), false);
  }
}
