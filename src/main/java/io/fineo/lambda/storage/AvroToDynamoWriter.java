package io.fineo.lambda.storage;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.base.Joiner;
import io.fineo.internal.customer.BaseFields;
import io.fineo.lambda.avro.FirehoseClientProperties;
import io.fineo.schema.avro.AvroRecordDecoder;
import io.fineo.schema.avro.AvroSchemaBridge;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;

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
 * <tr><td>[orgID][schema cannonical name]</td><td>[timestamp]</td><td>encoded with
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
 */
public class AvroToDynamoWriter {
  private static final Log LOG = LogFactory.getLog(AvroToDynamoWriter.class);
  private static final Joiner COMMAS = Joiner.on(',');
  static final String PARTITION_KEY_NAME = "id_schema";
  static final String SORT_KEY_NAME = "timestamp";
  private static final int LONG_SIZE = 8;
  private final DynamoTableManager tables;

  private final AmazonDynamoDBAsyncClient client;

  private final Phaser phase = new Phaser();
  private final List<UpdateItemRequest> actions = new LinkedList<>();

  public AvroToDynamoWriter(AmazonDynamoDBAsyncClient client, String dynamoIngestTablePrefix, long
   writeMax, long readMax) {
    this.client = client;
    this.tables = new DynamoTableManager(client, dynamoIngestTablePrefix, readMax, writeMax);
  }

  public static AvroToDynamoWriter create(FirehoseClientProperties props) {
    AmazonDynamoDBAsyncClient client = props.getDynamo();
    return new AvroToDynamoWriter(client, props.getDynamoIngestTablePrefix(),
      props.getDynamoWriteMax(), props.getDynamoReadMax());
  }

  /**
   * Write the record to dynamo. Completes asynchronously, call {@link #flush()} to ensure all
   * records finish writing to dynamo. <b>non-blocking, thread-safe</b>.
   *
   * @param record record to write to dynamo. Expected to have at least a {@link BaseFields} field
   */
  public void write(GenericRecord record) throws IOException, ExecutionException {
    UpdateItemRequest request = getUpdateForTable(record);

    // submit the request for the update items into a future. Handler does all the heavy lifting
    // of resubmission, etc.
    UpdateItemHandler handler = new UpdateItemHandler(request);
    actions.add(request);
    phase.register();
    submit(handler, request);
  }

  private void submit(UpdateItemHandler handler, UpdateItemRequest request) {
    client.updateItemAsync(request, handler);
  }

  /**
   * Pull out the timestamp from the record to find the table. Then setup the partition and and
   * sort key, so we just need to update the rest of the fields from the record
   *
   * @param record to parse
   * @return
   */
  private UpdateItemRequest getUpdateForTable(GenericRecord record) throws IOException {
    AvroRecordDecoder decoder = new AvroRecordDecoder(record);
    AvroRecordDecoder.RecordMetadata metadata = decoder.getMetadata();

    UpdateItemRequest request = new UpdateItemRequest();
    BaseFields fields = decoder.getBaseFields();

    String tableName = tables.getTableName(fields.getTimestamp());
    request.setTableName(tableName);

    request.addKeyEntry(PARTITION_KEY_NAME, getPartitionKey(metadata));
    request.addKeyEntry(SORT_KEY_NAME, getSortKey(fields));

    Map<String, List<String>> expressionBuilder = new HashMap<>();
    Map<String, AttributeValue> values = new HashMap<>();
    // store the unknown fields from the base fields that we parsed
    Map<String, String> unknown = fields.getUnknownFields();
    unknown.forEach((name, value) -> {
      AttributeValue attrib = new AttributeValue(value);
      setAttribute(name, attrib, expressionBuilder, values);
    });

    // for each field in the record, add it to the update, skipping the 'base fields' field,
    // since we handled that separately above
    record.getSchema().getFields().stream()
          .filter(field -> !field.name().equals(AvroSchemaBridge.BASE_FIELDS_KEY))
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
    return request;
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

    // convert the value into a hex so we can get an ExpressionAttributeValue
    String attributeName = String.format("%x", name);
    while (values.get(attributeName) != null) {
      attributeName += "1";
    }
    values.put(attributeName, value);
    set.add(name + "= :" + attributeName);
  }

  private AttributeValue getSortKey(BaseFields fields) {
    return new AttributeValue().withN(fields.getTimestamp().toString());
  }

  private AttributeValue getPartitionKey(AvroRecordDecoder.RecordMetadata metadata)
    throws IOException {
    return new AttributeValue(metadata.getOrgID() + metadata.getMetricCannonicalType());
  }

  /**
   * Blocking flush waiting on outstanding record updates
   */
  public void flush() {
    phase.register();
    phase.arriveAndAwaitAdvance();
    assert actions.size() == 0 :
      "Some outstanding actions, but phaser is done. Actions: " + actions;
    LOG.debug("All update actions completed!");
  }

  private class UpdateItemHandler implements AsyncHandler<UpdateItemRequest, UpdateItemResult> {

    private final UpdateItemRequest request;

    public UpdateItemHandler(UpdateItemRequest request) {
      this.request = request;
    }

    @Override
    public void onError(Exception exception) {
      LOG.error("Failed to make an update.", exception);
      submit(this, request);
    }

    @Override
    public void onSuccess(UpdateItemRequest request, UpdateItemResult updateItemResult) {
      // remove the request from the pending list because we were successful
      actions.remove(this.request);
      phase.arriveAndDeregister();
    }
  }
}
