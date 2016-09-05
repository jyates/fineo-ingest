package io.fineo.lambda.util;

import com.google.common.base.Joiner;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

/**
 * Utility for interacting with canonical schema
 */
public class SchemaUtil {

  private final SchemaStore store;
  private StoreClerk clerk;
  private String metricId;

  public SchemaUtil(SchemaStore store, GenericRecord record){
    this.store = store;
    read(record);
  }

  public static String toString(List<GenericRecord> records) {
    return "[" + Joiner.on(',')
                       .join(records.stream().map(SchemaUtil::toString).toArray()) + "]";
  }

  private static String toString(GenericRecord record) {
    StringBuffer sb = new StringBuffer("GR:{");
    Schema s = record.getSchema();
    sb.append("\nschema: " + s);
    sb.append("\n\t{\n");
    s.getFields().forEach(field -> {
      sb.append(field.name() + " -> " + record.get(field.name()) + "\n");
    });
    sb.append("\t}");
    sb.append("}");
    return sb.toString();
  }

  public void read(GenericRecord record) {
    RecordMetadata metadata = RecordMetadata.get(record);

    this.clerk = new StoreClerk(store, metadata.getOrgID());
    this.metricId = metadata.getMetricCanonicalType();
  }

  public String getCanonicalFieldName(String aliasName) {
    StoreClerk.Metric metric = clerk.getMetricForCanonicalName(metricId);
    return metric.getCanonicalNameFromUserFieldName(aliasName);
  }
}
