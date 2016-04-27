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
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.RecordMetadata;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Write {@link BaseFields} based avro-records into dynamo.
 * <p>
 * The name of the table is: [configured prefix]_[start time]-[end time] where start/end are
 * simply linux timestamps (names required to match pattern: [a-zA-Z0-9_.-]+).
 * <p/>
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
 * </p>
 */
public class AvroToDynamoWriter {
  private static final Log LOG = LogFactory.getLog(AvroToDynamoWriter.class);
  private static final Joiner COMMAS = Joiner.on(',');

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
    AwsAsyncRequest<GenericRecord, UpdateItemRequest> request = getUpdateForTable(record);
    this.submitter.submit(request);
  }

  public MultiWriteFailures<GenericRecord> flush() {
    return this.submitter.flush();
  }

  /**
   * Pull out the timestamp from the record to find the table. Then setup the partition and and
   * sort key, so we just need to update the rest of the fields from the record
   *
   * @param record to parse
   * @return
   */
  private AwsAsyncRequest<GenericRecord, UpdateItemRequest> getUpdateForTable(
    GenericRecord record) {
    RecordMetadata metadata = RecordMetadata.get(record);

    UpdateItemRequest request = new UpdateItemRequest();
    BaseFields fields = metadata.getBaseFields();

    String tableName = tables.getTableAndEnsureExists(fields.getTimestamp());
    request.setTableName(tableName);

    request.addKeyEntry(Schema.PARTITION_KEY_NAME, getPartitionKey(metadata));
    request.addKeyEntry(Schema.SORT_KEY_NAME, getSortKey(fields));

    Map<String, List<String>> expressionBuilder = new HashMap<>();
    Map<String, AttributeValue> values = new HashMap<>();

    // store the unknown fields from the base fields that we parsed
    Map<String, String> unknown = fields.getUnknownFields();
    unknown.forEach((name, value) -> {
      setAttribute(name, new AttributeValue(value), expressionBuilder, values);
    });

    // for each field in the record, add it to the update, skipping the 'base fields' field,
    // since we handled that separately above
    record.getSchema().getFields().stream()
          .filter(field -> !field.name().equals(AvroSchemaEncoder.BASE_FIELDS_KEY))
          .forEach(field -> {
            String name = field.name();
            setAttribute(name, io.fineo.lambda.dynamo.avro.Schema
              .convertField(field, record.get(name)), expressionBuilder, values);
          });

    // add a default field, just in case there are no fields in the record
    if (expressionBuilder.size() == 0) {
      setAttribute(Schema.MARKER, new AttributeValue("0"), expressionBuilder, values);
    }

    // convert each part of the expression into a single expression
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, List<String>> part : expressionBuilder.entrySet()) {
      sb.append(part.getKey() + " " + COMMAS.join(part.getValue()));
    }
    request.withUpdateExpression(sb.toString());
    // and the values for that expression
    request.withExpressionAttributeValues(values);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Record: " + record);
      LOG.debug("Using update: " + request.getUpdateExpression());
      LOG.debug("Using expression values: " + request.getExpressionAttributeValues());
    }

    return new AwsAsyncRequest<>(record, request);
  }

  private void setAttribute(String name, AttributeValue value, Map<String, List<String>> expression,
    Map<String, AttributeValue> values) {
    List<String> set = expression.get("SET");
    if (set == null) {
      set = new ArrayList<>();
      expression.put("SET", set);
    }

    // convert the name into a unique value so we can get an ExpressionAttributeValue
    String attributeName = DynamoExpressionPlaceHolders.asExpressionAttributeValue(name);
    while (values.containsKey(attributeName)) {
      attributeName += "a";
    }
    values.put(attributeName, value);
    set.add(name + "= " + attributeName);
  }

  private static AttributeValue getSortKey(BaseFields fields) {
    return new AttributeValue().withN(fields.getTimestamp().toString());
  }

  private static AttributeValue getPartitionKey(RecordMetadata metadata) {
    return Schema.getPartitionKey(metadata.getOrgID(), metadata.getMetricCanonicalType());
  }
}
