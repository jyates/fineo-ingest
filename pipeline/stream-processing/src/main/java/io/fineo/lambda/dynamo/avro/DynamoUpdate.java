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
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.fineo.lambda.dynamo.DynamoExpressionPlaceHolders.asExpressionAttributeValue;
import static io.fineo.lambda.dynamo.DynamoExpressionPlaceHolders.asExpressionName;
import static io.fineo.lambda.dynamo.avro.DynamoAvroRecordEncoder.convertField;
import static java.lang.String.format;

/**
 * Do the actual work of making an update for a single generic record
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
    AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, GenericRecord> submitter) {
    RecordMetadata metadata = RecordMetadata.get(record);
    this.fields = metadata.getBaseFields();

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

  private UpdateItemRequest getInitialRequest(UpdateItemRequest baseRequest) {
    this.initialRequest = baseRequest;

    List<String> setExpressions = new ArrayList<>();
    Map<String, String> names = new HashMap<>();
    Map<String, AttributeValue> values = new HashMap<>();

    // crypo-strong random generation. Its a bit long, but at least collisions are basically 0.
    this.id = "i" + UUID.randomUUID().toString();

    List<String> conditions = new ArrayList<>();
    // do the actual work of setting values
    handleValues((name, value) -> {
      setSimpleAttribute(name, value, names, values, setExpressions);
      conditions.add(format("attribute_exists(%s)", name));
    });

    // ensure that each of the map fields exists
    if(conditions.size() > 0) {
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
          .filter(field -> !field.name().equals(AvroSchemaEncoder.BASE_FIELDS_KEY))
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
