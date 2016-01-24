package io.fineo.lambda.dynamo;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Run multiple scans against (potentially) multiple dynamo DB tables.
 * <p>
 * Basically, this aggregates a bunch of iterators. As soon as we encounter an exception from a
 * request, we surface it to the client in the next call to {@link #hasNext()}
 * </p>
 */
public class MultiDynamoScanIterator implements Iterator<Map<String, AttributeValue>> {

  private Queue<ResultOrException<List<Map<String, AttributeValue>>>> nextQueue;
  private ResultOrException<List<Map<String, AttributeValue>>> next;
  private final List<ScanResultChannel<ResultOrException<List<Map<String, AttributeValue>>>>>
    channels;

  public MultiDynamoScanIterator(AmazonDynamoDBAsyncClient client, List<ScanRequest> requests) {
    channels = new ArrayList<>(requests.size());
    for (ScanRequest request : requests) {
      ScanResultChannel channel = new ScanResultChannel();
      channels.add(channel);
      client.scanAsync(request, new AsyncHandler<ScanRequest, ScanResult>() {
        // TODO support paging results
        @Override
        public void onError(Exception exception) {
          channel.offer(new ResultOrException<>().withException(exception));
          channel.close();
        }

        @Override
        public void onSuccess(ScanRequest request, ScanResult scanResult) {
          channel.offer(scanResult.getItems());
          channel.close();
        }
      });
    }

    // wait for the first channel to return a result before done
    getNext();

    // next is definitely not empty, so check to make sure it doesn't have an exception
    throwIfRequestFailed();
  }

  private void getNext() {
    while ((nextQueue == null || nextQueue.isEmpty()) && channels.size() > 0) {
      nextQueue = getNext(channels.remove(0));
    }
  }

  private Queue<ResultOrException<List<Map<String, AttributeValue>>>> getNext
    (ScanResultChannel<ResultOrException<List<Map<String, AttributeValue>>>> channel) {
    if (channel.isOpen()) {
      try {
        Queue<ResultOrException<List<Map<String, AttributeValue>>>> queue = new ArrayDeque<>(1);
        queue.add(channel.next());
      } catch (ClosedChannelException e) {
        return getNext(channel);
      }
    }
    return channel.drain();
  }

  private void throwIfRequestFailed() {
    ResultOrException<List<Map<String, AttributeValue>>> re = nextQueue.peek();
    if(re.hasException()){
      throw new RuntimeException(re.getException());
    }
  }

  @Override
  public boolean hasNext() {
    if(nextQueue.isEmpty()){
      getNext();
    }
    throwIfRequestFailed();
    .
  }

  @Override
  public Map<String, AttributeValue> next() {
    ResultOrException<List<>>
  }
}
