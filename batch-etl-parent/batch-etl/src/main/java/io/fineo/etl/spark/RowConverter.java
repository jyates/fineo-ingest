package io.fineo.etl.spark;

import io.fineo.internal.customer.BaseFields;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Function to convert GenericRecords into {@link Row Rows} based on the schema
 */
public class RowConverter implements Function<GenericRecord, Row>, Serializable {

  private final SerializableMetric rawMetric;
  private final String orgId;
  private final String metricName;
  private final Map<String, List<String>> canonicalNamesToAliases;

  public RowConverter(SerializableMetric rawMetric,
    Map<String, List<String>> canonicalNamesToAliases,
    String orgId, String metricCanonicalName) {
    this.rawMetric = rawMetric;
    this.canonicalNamesToAliases = canonicalNamesToAliases;
    this.orgId = orgId;
    this.metricName = metricCanonicalName;
  }

  @Override
  public Row call(GenericRecord record) throws Exception {
    StoreClerk.Metric metric = StoreClerk.Metric.metricOnlyFunctions(rawMetric.get());
    RecordMetadata metadata = RecordMetadata.get(record);
    BaseFields base = metadata.getBaseFields();
    List<Object> fields = new ArrayList<>();
    fields.add(orgId);
    fields.add(metricName);
    fields.add(base.getTimestamp());

    // populate the other fields, as we have them
    metric.getUserVisibleFields().stream()
          .map(field -> field.getCname())
          .forEach(canonicalName -> {
            Object fieldRecord = record.get(canonicalName);
            // unpack the actual value from the record for the field
            if (fieldRecord != null) {
              fields.add(((GenericRecord) fieldRecord).get(1));
            } else {
              // we don't know about that schema type, but maybe the schema has been updated
              // and have it as an alias
              List<String> aliases = this.canonicalNamesToAliases.get(canonicalName);
              for (String alias : aliases) {
                fieldRecord = base.getUnknownFields().get(alias);
                if (fieldRecord != null) {
                  fields.add(fieldRecord);
                  break;
                }
              }
            }
          });

    return RowFactory.create(fields.toArray());
  }
}
