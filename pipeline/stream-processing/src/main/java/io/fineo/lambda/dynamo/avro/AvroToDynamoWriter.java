package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.base.Joiner;
import io.fineo.internal.customer.BaseFields;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.DynamoExpressionPlaceHolders;
import io.fineo.lambda.dynamo.DynamoTableCreator;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.RecordMetadata;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.fineo.lambda.dynamo.avro.DynamoAvroRecordEncoder.convertField;


/**
 * Write {@link BaseFields} based avro-records into dynamo. Uses {@link DynamoTableCreator} to
 * create names based on a prefix.
 * <p>
 * Its assumed that we always know at least the orgID, if not also the schema canonical name.
 * Thus the partition key doesn't need a separator or length suffix.
 * <p/>
 * However, you should use a higher level api read and access records in Dynamo.
 * </p>
 * <p>
 * It's assumed that we don't have a large number of actions taken before calling flush.
 * Generally, this is fine as this writer is called from a short-running lambda function that
 * only handles a very small number of requests.
 * </p>
 * <p>
 * All writes are accumulated until a call to {@link #flush()}, which is <b>blocks until all
 * requests have completed</b>. This is merely a simple wrapper around an {@link AwsAsyncSubmitter}
 *
 * @see AwsAsyncSubmitter for more information about thread safety
 * @see DynamoTableCreator for information on table naming
 * </p>
 */
public class AvroToDynamoWriter {

  private final DynamoTableCreator tables;
  private final AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, GenericRecord> submitter;

  public AvroToDynamoWriter(AmazonDynamoDBAsyncClient client,
    long maxRetries, DynamoTableCreator creator) {
    this.submitter = new AwsAsyncSubmitter<>(maxRetries, client::updateItemAsync);
    this.tables = creator;
  }

  /**
   * Write the record to dynamo. Completes asynchronously, call {@link #flush()} to ensure all
   * records finish writing to dynamo. <b>non-blocking, thread-safe</b>.
   *
   * @param record record to write to dynamo. Expected to have at least a {@link BaseFields} field
   */
  public void write(GenericRecord record) {
    DynamoUpdate request = getUpdateForRecord(record);
    request.submit(this.submitter);
  }

  public MultiWriteFailures<GenericRecord> flush() {
    return this.submitter.flush();
  }

  private DynamoUpdate getUpdateForRecord(GenericRecord record) {
    return new DynamoUpdate(record, this::getTableForEvent);
  }

  /*
   * Pull out the timestamp from the record to find the table
   */
  private String getTableForEvent(BaseFields field) {
    return tables.getTableAndEnsureExists(field.getTimestamp());
  }
}
