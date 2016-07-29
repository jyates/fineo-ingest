package io.fineo.lambda.e2e.resources.aws.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import io.fineo.lambda.e2e.resources.ClosableSupplier;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Manages getting shard iterators for streams
 */
public class ShardIteratorManager {

  private static final Duration TIMEOUT = Duration.ofMinutes(4);
  private final AmazonKinesisAsyncClient kinesis;

  private Map<String, String> iterators = new HashMap<>();

  public ShardIteratorManager(AmazonKinesisAsyncClient kinesis) {
    this.kinesis = kinesis;
  }

  /**
   * @return a new shard iterator for the given stream
   */
  public ClosableSupplier<GetRecordsResult> getShardIterator(String stream) {
    String id = UUID.randomUUID().toString();
    iterators.put(id, null);

    return new ClosableSupplier<GetRecordsResult>() {
      private String consumed;
      private Instant start;

      @Override
      public void close() throws IOException {
        iterators.remove(id);
      }

      @Override
      public GetRecordsResult get() {
        // first time or shard expired, get a new shard
        if (iterators.get(id) == null || !start.plus(TIMEOUT).isBefore(Instant.now())) {
          String iterator;
          if (consumed == null) {
            iterator = kinesis.getShardIterator(stream, "0", ShardIteratorType.TRIM_HORIZON.name())
                              .getShardIterator();
          } else {
            iterator = kinesis
              .getShardIterator(stream, "0", ShardIteratorType.AFTER_SEQUENCE_NUMBER.name(),
                consumed).getShardIterator();
          }
          iterators.put(id, iterator);
          this.start = Instant.now();
        }
        try {
          GetRecordsResult result = kinesis.getRecords(
            new GetRecordsRequest().withShardIterator(iterators.get(id)));
          start = Instant.now();
          iterators.put(id, result.getNextShardIterator());
          int numRecords = result.getRecords().size();
          if (numRecords > 0) {
            consumed = result.getRecords().get(numRecords - 1).getSequenceNumber();
          }
          return result;
        } catch (ExpiredIteratorException e) {
          // clear the previous iterator and get a new one
          iterators.put(id, null);
          return get();
        }
      }
    };
  }
}
