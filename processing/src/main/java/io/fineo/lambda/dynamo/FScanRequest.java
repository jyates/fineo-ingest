package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

import java.util.Map;

/**
 * Wrapper around a {@link ScanRequest} but also supports a stop key
 */
public class FScanRequest {

  private final ScanRequest scan;
  private String stop;

  public FScanRequest(String table){
    this.scan = new ScanRequest(table);
  }

  public void setStopKey(String stop){
    this.stop = stop;
  }

  public void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
    this.scan.setExclusiveStartKey(exclusiveStartKey);
  }

  public void setConsistentRead(boolean consistentRead) {
    this.scan.setConsistentRead(consistentRead);
  }

  public ScanRequest getScan() {
    return this.scan;
  }

  public String getStopKey() {
    return stop;
  }
}
