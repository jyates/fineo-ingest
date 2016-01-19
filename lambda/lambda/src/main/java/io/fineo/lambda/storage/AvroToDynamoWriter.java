package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.base.Joiner;
import io.fineo.internal.customer.BaseFields;
import io.fineo.lambda.avro.LambdaClientProperties;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.schema.avro.AvroRecordDecoder;
import io.fineo.schema.avro.AvroSchemaEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Write {@link BaseFields} based avro-records into dynamo.
 * <p>
 * The name of the table is: [configured prefix]_[start time]-[end time] where start/end are
 * simply linux timestamps (names required to match pattern: [a-zA-Z0-9_.-]+).
 * <p/>
 * <p>
 * The schema of the table is as follows:
 * <table>
 * <tr><th>Partition Key</th><th>Sort Key</th><th>Known Fields</th><th>Unknown Fields</th></tr>
 * <tr><td>[orgID]_[metricId]</td><td>[timestamp]</td><td>encoded as
 * type</td><td>string
 * encoded</td></tr>
 * </table>
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
 * <p/>
 * All writes are accumulated until a call to {@link #flush()}, which is <b>blocks until all
 * requests have completed</b>. This is merely a simple wrapper around an {@link AwsAsyncSubmitter}
 *
 * @see AwsAsyncSubmitter for more information about thread safety
 * </p>
 */
public class AvroToDynamoWriter {
  private static final Log LOG = LogFactory.getLog(AvroToDynamoWriter.class);
  private static final Joiner COMMAS = Joiner.on(',');
  /**
   * name of the column shortened (for speed) of "org ID" and "metric id"
   */
  static final String PARTITION_KEY_NAME = "oid_mid";
  /**
   * shortened for 'timestamp'
   */
  static final String SORT_KEY_NAME = "ts";
  // TODO replace with a schema ID so we can lookup the schema on read, if necessary
  private static final String MARKER = "marker";

  private final DynamoTableManager tables;
  private final AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, GenericRecord> submitter;

  private AvroToDynamoWriter(AmazonDynamoDBAsyncClient client, String dynamoIngestTablePrefix, long
    writeMax, long readMax, long maxRetries) {
    this.submitter = new AwsAsyncSubmitter<>(maxRetries, client::updateItemAsync);
    this.tables = new DynamoTableManager(client, dynamoIngestTablePrefix, readMax, writeMax);
  }

  public static AvroToDynamoWriter create(LambdaClientProperties props) {
    AmazonDynamoDBAsyncClient client = props.getDynamo();
    return new AvroToDynamoWriter(client, props.getDynamoIngestTablePrefix(),
      props.getDynamoWriteMax(), props.getDynamoReadMax(), props.getDynamoMaxRetries());
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
    AvroRecordDecoder decoder = new AvroRecordDecoder(record);
    AvroRecordDecoder.RecordMetadata metadata = decoder.getMetadata();

    UpdateItemRequest request = new UpdateItemRequest();
    BaseFields fields = decoder.getBaseFields();

    String tableName = tables.getTableAndEnsureExists(fields.getTimestamp());
    request.setTableName(tableName);

    request.addKeyEntry(PARTITION_KEY_NAME, getPartitionKey(metadata));
    request.addKeyEntry(SORT_KEY_NAME, getSortKey(fields));

    Map<String, List<String>> expressionBuilder = new HashMap<>();
    Map<String, AttributeValue> values = new HashMap<>();

    // add a default field, just in case there are no fields in the record
    setAttribute(MARKER, new AttributeValue("0"), expressionBuilder, values);

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
            setAttribute(name, convertField(field, record.get(name)), expressionBuilder, values);
          });

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

  private AttributeValue convertField(Schema.Field field, Object value) {
    Schema.Type type = field.schema().getType();
    switch (type) {
      case STRING:
        return new AttributeValue(String.valueOf(value));
      case BYTES:
        return new AttributeValue().withB(ByteBuffer.wrap((byte[]) value));
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new AttributeValue().withN(value.toString());
      case BOOLEAN:
        return new AttributeValue().withBOOL(Boolean.valueOf(value.toString()));
      default:
        return null;
    }
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

  private AttributeValue getSortKey(BaseFields fields) {
    return new AttributeValue().withN(fields.getTimestamp().toString());
  }

  private AttributeValue getPartitionKey(AvroRecordDecoder.RecordMetadata metadata) {
    return new AttributeValue(metadata.getOrgID() + metadata.getMetricCannonicalType());
  }

  public static List<GenericRecord> getFailedRecords(MultiWriteFailures<GenericRecord>
    failures) {
    return failures.getActions().parallelStream()
                   .map(handler -> handler.getBaseRecord())
                   .collect(Collectors.toList());
  }
}