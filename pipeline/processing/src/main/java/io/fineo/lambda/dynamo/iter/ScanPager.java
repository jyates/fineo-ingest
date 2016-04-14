package io.fineo.lambda.dynamo.iter;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import io.fineo.lambda.dynamo.ResultOrException;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * Page a scan across a dynamoDB table. Page size is determined by the provided {@link
 * ScanRequest}. This just handles asynchronously providing the result (or exception) to the
 * result queue.
 * <p>
 * Note that the {@link ScanRequest} will be changed over the course of this scanner - a copy
 * should be made if you want to reuse it.
 * </p>
 */
public class ScanPager extends BasePager<ResultOrException<Map<String, AttributeValue>>> {

  private final AmazonDynamoDBAsyncClient client;
  private final ScanRequest scan;
  private final String pkName;
  private final String stopKey;

  public ScanPager(AmazonDynamoDBAsyncClient client, ScanRequest scan, String partitionKeyName,
    String stopKey) {
    this.client = client;
    this.scan = scan;
    this.pkName = partitionKeyName;
    this.stopKey = stopKey;
  }

  @Override
  public void page(Queue<ResultOrException<Map<String, AttributeValue>>> queue) {
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
                    .filter(map -> stopKey == null ||
                                   map.get(pkName).getS().compareTo(stopKey) < 0)
                    .map(map -> new ResultOrException<>(map)).collect(Collectors.toList());

        // return all the values
        queue.addAll(result);
        batchComplete();

        // we dropped off the end of the results that we care about
        if (result.size() < scanResult.getCount() ||
            scanResult.getLastEvaluatedKey() == null ||
            scanResult.getLastEvaluatedKey().isEmpty()) {
          complete();
        } else {
          // there are (possibly - DynamoDB documentation about last evaluated key mgmt) more
          // results
          scan.setExclusiveStartKey(scanResult.getLastEvaluatedKey());
        }
      }
    });
  }
}
