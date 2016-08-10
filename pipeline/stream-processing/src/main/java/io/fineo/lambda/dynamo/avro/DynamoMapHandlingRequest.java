package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import io.fineo.lambda.aws.AwsAsyncRequest;
import org.apache.avro.generic.GenericRecord;

/**
 *
 */
public class DynamoMapHandlingRequest extends AwsAsyncRequest<GenericRecord, UpdateItemRequest> {
  public DynamoMapHandlingRequest(GenericRecord base,
    UpdateItemRequest request) {
    super(base, request);
  }

  @Override
  public boolean onError(Exception exception) {
//    if(exception instanceof AmazonServiceException && ((AmazonServiceException) The document path provided in the update expression is invalid for update)

    return false;
  }
}
