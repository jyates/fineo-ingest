package io.fineo.lambda.dynamo.iter;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import io.fineo.lambda.dynamo.ResultOrException;
import io.fineo.lambda.dynamo.avro.Schema;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Page a scan across a dynamoDB table. Page size is determined by the provided {@link
 * ScanRequest}. This just handles asynchronously providing the result (or exception) to the
 * result queue in {@link #page(Pipe)}. All elements are added to the {@link Pipe} in a single
 * call, either {@link Pipe#add(Object)} or {@link Pipe#addAll(Collection)}.
 * <p>
 * Note that the {@link ScanRequest} will be changed over the course of this scanner - a copy
 * should be made if you want to reuse it.
 * </p>
 *
 * @see Pipe
 */
public class PagingScanRunner {

  private AmazonDynamoDBAsyncClient client;
  private ScanRequest scan;
  private String stopKey;
  private boolean complete;

  public PagingScanRunner(AmazonDynamoDBAsyncClient client,
    ScanRequest scan, String stopKey) {
    this.client = client;
    this.scan = scan;
    this.stopKey = stopKey;
  }

  public boolean complete() {
    return this.complete;
  }

  /**
   * Get the next page of results
   *
   * @param queue to receive the results
   */
  public void page(Pipe<ResultOrException<Map<String, AttributeValue>>> queue) {
    client.scanAsync(scan, new AsyncHandler<ScanRequest, ScanResult>() {
      @Override
      public void onError(Exception exception) {
        queue.add(new ResultOrException<>(exception));
      }

      @Override
      public void onSuccess(ScanRequest request, ScanResult scanResult) {
        // remove all the elements that are outside the range
        List<ResultOrException<Map<String, AttributeValue>>> result =
          scanResult.getItems().stream()
                    .filter(map -> stopKey != null ||
                                   map.get(Schema.PARTITION_KEY_NAME).getS().compareTo(stopKey) < 0)
                    .map(map -> new ResultOrException<>(map)).collect(Collectors.toList());

        // we dropped off the end of the results that we care about
        complete = result.size() < scanResult.getCount() ||
                   scanResult.getLastEvaluatedKey() == null ||
                   scanResult.getLastEvaluatedKey().isEmpty();
        // there are (possibly - DynamoDB documentation about last evaluated key mgmt) more results
        if (!complete) {
          scan.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
        }

        queue.addAll(result);
      }
    });
  }
}