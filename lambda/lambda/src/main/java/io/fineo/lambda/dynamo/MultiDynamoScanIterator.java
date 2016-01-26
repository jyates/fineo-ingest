package io.fineo.lambda.dynamo;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Run multiple scans against (potentially) multiple dynamo DB tables.
 * <p>
 * Basically, this aggregates a bunch of iterators. As soon as we encounter an exception from a
 * request, we surface it to the client in the next call to {@link #hasNext()}
 * </p>
 */
public class MultiDynamoScanIterator implements Iterator<Map<String, AttributeValue>> {

  private ResultOrException<Iterator<Map<String, AttributeValue>>> nextBatch;
  private Iterator<Map<String, AttributeValue>> next;
  private final List<ResultOrException<Iterator<Map<String, AttributeValue>>>> results;

  public MultiDynamoScanIterator(AmazonDynamoDBAsyncClient client, List<ScanRequest> requests) {
    results = new LinkedList<>();
    for (ScanRequest request : requests) {
      ResultOrException<Iterator<Map<String, AttributeValue>>> result = new ResultOrException<>();
      results.add(result);
      client.scanAsync(request, new AsyncHandler<ScanRequest, ScanResult>() {
        // TODO support paging results
        @Override
        public void onError(Exception exception) {
          result.setException(exception);
        }

        @Override
        public void onSuccess(ScanRequest request, ScanResult scanResult) {
          result.setResult(scanResult.getItems().iterator());
        }
      });
    }

    // wait for the first channel to return a result before done
    next = getNext();
  }

  private Iterator<Map<String, AttributeValue>> getNext() {
    // get the next batch from the results we are tracking
    while (nextBatch == null && results.size() > 0) {
      nextBatch = results.remove(0);
    }

    // exit if we don't have any more results to iterate
    if (nextBatch == null) {
      return null;
    }

    // wait for the results to be set for the batch we want to read
    try {
      nextBatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // result is set, but its an exception, so throw it back up to the client
    if (nextBatch.hasException()) {
      throw new RuntimeException(nextBatch.getException());
    }

    return nextBatch.getResult().hasNext() ? nextBatch.getResult() : getNext();
  }

  @Override
  public boolean hasNext() {
    if (next == null || !next.hasNext()) {
      next = getNext();
    }

    return next != null;
  }

  @Override
  public Map<String, AttributeValue> next() {
    return next.next();
  }
}