package io.fineo.lambda.dynamo.avro;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.base.Joiner;
import io.fineo.internal.customer.BaseFields;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.dynamo.DynamoExpressionPlaceHolders;
import io.fineo.lambda.dynamo.Schema;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.AvroSchemaProperties;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.fineo.lambda.dynamo.DynamoExpressionPlaceHolders.asExpressionAttributeValue;
import static io.fineo.lambda.dynamo.DynamoExpressionPlaceHolders.asExpressionName;
import static io.fineo.lambda.dynamo.avro.DynamoAvroRecordEncoder.convertField;
import static java.lang.String.format;

/**
 * Update a single row in dynamo with a single record.
 * <p>
 * This is actually more complex than it sounds because dynamo only has a single view of a row,
 * not multiple versions as with Cassandra/HBase. Thus, we need to implement versioning on our
 * own. This is done with two things:
 * <ol>
 * <li>set of metrics ids</li>
 * <li>map for each metric field from id -> value</li>
 * </ol>
 * On read we can then undo the mapping from id -> values. Its assumed this is all
 * semi-transactional because it happens in the same row. Either the ID and its added is added on
 * merge (eventual consistency) or its not present.
 * </p>
 * <p>
 * The process of how we implement the write is a bit arduous. It proceeds roughly like this:
 * <ol>
 *   <li>Attempt to update an entry in the map</li>
 *   <li>That fails, attempt to create the map with the item being the only thing there</li>
 *   <lo>That fails, reattempt to update the map - someone beat us there.</lo>
 * </ol>
 * Naturally, there are a host of possible errors, so we have to sure to catch the right one and
 * then implement the correct error handling based on what step/condition actually failed.
 * </p>
 * The state-machine update logic is handled in the {@link DynamoMapUpdater}, though the actual
 * work of <i>how</i> each step proceeds is handled here (via lambda method references).
 *
 * @see DynamoMapUpdater
 */
public class DynamoUpdate {

  final static String FAILED_INITIAL_UPDATE_MESSAGE =
    "The document path provided in the update expression is invalid for update";
  private static final Logger LOG = LoggerFactory.getLogger(DynamoUpdate.class);
  private static final Joiner COMMAS = Joiner.on(", ");

  private final GenericRecord record;
  private final Function<BaseFields, String> table;
  private String id;
  private UpdateItemRequest initialRequest;
  private BaseFields fields;

  public DynamoUpdate(GenericRecord record, Function<BaseFields, String> tableGetter) {
    this.record = record;
    this.table = tableGetter;
  }

  public void submit(
    AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, GenericRecord> submitter)
    throws UnsupportedEncodingException, NoSuchAlgorithmException {
    RecordMetadata metadata = RecordMetadata.get(record);
    this.fields = metadata.getBaseFields();

    this.id = getId();

    UpdateItemRequest baseRequest = new UpdateItemRequest();
    baseRequest.setTableName(table.apply(fields));
    baseRequest.addKeyEntry(Schema.PARTITION_KEY_NAME, getPartitionKey(metadata));
    baseRequest.addKeyEntry(Schema.SORT_KEY_NAME, getSortKey(fields));

    DynamoMapUpdater<GenericRecord> updater = new DynamoMapUpdater<>(
      (record) -> this.getInitialRequest(baseRequest),
      this::getMapSetterRequest,
      this::getSetMapFieldsRequest,
      submitter);
    updater.submit(record, baseRequest.getTableName());
  }

  /**
   * Generate a unique Id for the current record
   */
  private String getId() throws NoSuchAlgorithmException, UnsupportedEncodingException {
    StringBuffer sb = new StringBuffer(fields.getTimestamp().toString());
    org.apache.avro.Schema schema = record.getSchema();
    // collect the values of all the fields
    schema.getFields().stream()
          .map(org.apache.avro.Schema.Field::name)
          .filter(name -> !AvroSchemaProperties.BASE_FIELDS_KEY.equals(name))
          .map(name -> {
            GenericData.Record rec = (GenericData.Record) record.get(name);
            return rec.get("value");
          }).forEach(obj -> sb.append(obj.toString()));
    for (Map.Entry<String, String> e : this.fields.getUnknownFields().entrySet()) {
      sb.append(e.getKey());
      sb.append(e.getValue());
    }
    return toHexString(MessageDigest.getInstance("MD5").digest(sb.toString().getBytes("UTF-8")));
  }

  private static String toHexString(byte[] bytes) {
    StringBuffer hexString = new StringBuffer();

    for (int i = 0; i < bytes.length; i++) {
      String hex = Integer.toHexString(0xFF & bytes[i]);
      hexString.append(hex);
    }

    return hexString.toString();
  }

  private UpdateItemRequest getInitialRequest(UpdateItemRequest baseRequest) {
    this.initialRequest = baseRequest;

    List<String> setExpressions = new ArrayList<>();
    Map<String, String> names = new HashMap<>();
    Map<String, AttributeValue> values = new HashMap<>();

    List<String> conditions = new ArrayList<>();
    // do the actual work of setting values
    handleValues((name, value) -> {
      setSimpleAttribute(name, value, names, values, setExpressions);
      conditions.add(format("attribute_exists(%s)", name));
    });

    // ensure that each of the map fields exists
    if (conditions.size() > 0) {
      initialRequest.setConditionExpression(Joiner.on(" AND ").join(conditions));
    }
    return finalizeRequest(initialRequest, names, values, setExpressions);
  }

  private UpdateItemRequest getMapSetterRequest(UpdateItemRequest prevRequest) {
    UpdateItemRequest request = new UpdateItemRequest();
    request.setTableName(prevRequest.getTableName());
    request.setKey(prevRequest.getKey());

    Map<String, String> names = new HashMap<>();
    Map<String, AttributeValue> values = new HashMap<>();

    // create a map with the expected value for each field. If there are concurrent attempt here,
    // one of them will win and one of them will not be set (unless there is split dynamo logic,
    // in which case, both of them could succeed).
    List<String> setExpressions = new ArrayList<>();
    handleValues((name, value) -> {
      // the simple name/value gets transformed into a map expression of {id -> value}
      String mapAlias = asExpressionName(name);
      names.put(mapAlias, name);
      String valueAlias = DynamoExpressionPlaceHolders.asExpressionAttributeValue(name + "_value");
      Map<String, AttributeValue> map = new HashMap<>();
      map.put(id, value);
      AttributeValue mapValue = new AttributeValue().withM(map);
      values.put(valueAlias, mapValue);
      setExpressions.add(format("%s = if_not_exists(%s, %s)", mapAlias, mapAlias, valueAlias));
    });
    request.withReturnValues(ReturnValue.UPDATED_NEW);
    LOG.debug("Setting maps with names: {}", names);
    return finalizeRequest(request, names, values, setExpressions);
  }

  /**
   * Just the work of setting the fields that are different than those we expected to find. This
   * happens when we have concurrent updates to fields, of which only 1 set will win for each field.
   */
  private UpdateItemRequest getSetMapFieldsRequest(UpdateItemRequest previous,
    UpdateItemResult result) {
    UpdateItemRequest request = new UpdateItemRequest();
    request.setKey(previous.getKey());

    Map<String, String> names = new HashMap<>();
    request.setExpressionAttributeNames(names);
    Map<String, AttributeValue> values = new HashMap<>();
    request.setExpressionAttributeValues(values);

    Map<String, AttributeValue> previousUpdates = result.getAttributes();
    List<String> expressions = new ArrayList<>();
    boolean[] set = new boolean[]{true};
    handleValues((name, value) -> {
      AttributeValue mapVal = previousUpdates.get(name);
      AttributeValue storedValue = mapVal.getM().get(id);
      if (storedValue == null) {
        set[0] = false;
        setSimpleAttribute(name, value, names, values, expressions);
      } else if (!storedValue.equals(value)) {
        throw new RuntimeException("Got an existing value for id: " + id + ", but it doesn't match "
                                   + "the attribute we were trying to set!");
      }
    });

    // all done - everything was set!
    if (set[0]) {
      return null;
    }

    return finalizeRequest(request, names, values, expressions);
  }

  private UpdateItemRequest finalizeRequest(UpdateItemRequest request, Map<String, String> names,
    Map<String, AttributeValue> values, List<String> setExpressions) {
    // add the id setting for every request. Since its a set, this is fine
    String idFieldName = asExpressionName(Schema.ID_FIELD);
    names.put(idFieldName, Schema.ID_FIELD);
    String rowIdValue = asExpressionAttributeValue(id);
    values.put(rowIdValue, new AttributeValue().withSS(id));
    String update = format("ADD %s %s", idFieldName, rowIdValue);

    if (setExpressions.size() > 0) {
      // convert each part of the expression into a single expression
      StringBuilder sb = new StringBuilder(" SET ");
      COMMAS.appendTo(sb, setExpressions);
      update += sb.toString();
    }

    request.withUpdateExpression(update);
    request.withExpressionAttributeNames(names);
    request.withExpressionAttributeValues(values);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Record: {}", record);
      LOG.trace("Update key: {}", request.getKey());
      LOG.trace("Using update: {}", request.getUpdateExpression());
      LOG.trace("With condition: {}", request.getConditionExpression());
      LOG.trace("Using expression names: {}", request.getExpressionAttributeNames());
      LOG.trace("Using expression values: {}", request.getExpressionAttributeValues());
      LOG.trace("With return: {}", request.getReturnValues());
    }
    return request;
  }

  private void setSimpleAttribute(String name, AttributeValue value, Map<String, String> names,
    Map<String, AttributeValue> values, List<String> set) {
    String aliasName = asExpressionName(name);
    names.put(aliasName, name);
    // convert the name into a unique value so we can get an ExpressionAttributeValue
    String attributeName = asExpressionAttributeValue(name);
    while (values.containsKey(attributeName)) {
      attributeName += "a";
    }
    values.put(attributeName, value);
    String rowIdName = asExpressionName(id);
    names.put(rowIdName, id);
    set.add(format("%s.%s = %s", aliasName, rowIdName, attributeName));
  }

  private boolean handleValues(BiConsumer<String, AttributeValue> handler) {
    boolean[] hasFields = new boolean[]{false};
    Map<String, String> unknown = fields.getUnknownFields();
    unknown.forEach((name, value) -> {
      handler.accept(name, new AttributeValue(value));
      hasFields[0] = true;
    });

    // for each field in the record, add it to the update, skipping the 'base fields' field,
    // since we handled that separately above
    record.getSchema().getFields().stream()
          .filter(field -> !field.name().equals(AvroSchemaProperties.BASE_FIELDS_KEY))
          .forEach(field -> {
            hasFields[0] = true;
            Pair<String, AttributeValue> attribute =
              convertField((GenericData.Record) record.get(field.name()));
            handler.accept(attribute.getKey(), attribute.getValue());
          });
    return hasFields[0];
  }

  private static AttributeValue getSortKey(BaseFields fields) {
    return new AttributeValue().withN(fields.getTimestamp().toString());
  }

  private static AttributeValue getPartitionKey(RecordMetadata metadata) {
    return Schema.getPartitionKey(metadata.getOrgID(), metadata.getMetricCanonicalType());
  }
}
