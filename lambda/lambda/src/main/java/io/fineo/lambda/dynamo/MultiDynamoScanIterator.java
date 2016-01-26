package io.fineo.lambda.dynamo;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import io.fineo.lambda.dynamo.avro.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

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
  private final List<Iterator> iterators;

  public MultiDynamoScanIterator(AmazonDynamoDBAsyncClient client, List<FScanRequest> requests) {
    results = new LinkedList<>();
    iterators = new ArrayList<>(requests.size());
    for (FScanRequest request : requests) {
      ResultOrException<Iterator<Map<String, AttributeValue>>> result = new ResultOrException<>();
      iterators.add(new PagingIterator<>(10, new Scanner(result, client, request)));
      results.add(result);
    }

    // wait for the first iterator to return a result before done
    next = getNext();
  }

  private class Scanner
    implements BiFunction<Queue<Map<String, AttributeValue>>, PagingIterator<Map<String,
    AttributeValue>>, Void> {
    private final ResultOrException result;
    private final AmazonDynamoDBAsyncClient client;
    private final FScanRequest scan;
    private boolean first = true;

    public Scanner(ResultOrException result, AmazonDynamoDBAsyncClient client, FScanRequest scan) {
      this.result = result;
      this.client = client;
      this.scan = scan;
    }

    @Override
    public Void apply(Queue<Map<String, AttributeValue>> queue,
      PagingIterator<Map<String, AttributeValue>> pagingIterator) {
      client.scanAsync(scan.getScan(), new AsyncHandler<ScanRequest, ScanResult>() {
        @Override
        public void onError(Exception exception) {
          result.setException(exception);
        }

        @Override
        public void onSuccess(ScanRequest request, ScanResult scanResult) {
          if (first) {
            result.setResult(pagingIterator);
            first = false;
          }
          // remove all the elements that are outside the range
          List<Map<String, AttributeValue>> result = scanResult.getItems().stream().filter(
            map -> map.get(Schema.PARTITION_KEY_NAME).getS().compareTo(scan.getStopKey()) < 0)
                                                               .collect(Collectors.toList());
          queue.addAll(result);
          // we dropped off the end of the results that we care about
          if (result.size() < scanResult.getCount() || scanResult.getLastEvaluatedKey().isEmpty()) {
            pagingIterator.done();
          }
          // there may still be more results
          else {
            scan.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
          }
        }

      });
      return null;
    }
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