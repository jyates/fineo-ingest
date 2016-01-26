package io.fineo.lambda.util;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

/**
 * Utility for interacting with canonical schema
 */
public class SchemaUtil {

  private final SchemaStore store;
  private Metric metric;

  public SchemaUtil(SchemaStore store, GenericRecord record){
    this.store = store;
    read(record);
  }

  public void read(GenericRecord record) {
    this.metric = store.getMetricMetadata(RecordMetadata.get(record));
  }

  public String getCanonicalName(String aliasName) {
    Preconditions.checkArgument(!AvroSchemaEncoder.IS_BASE_FIELD.test(aliasName),
      "Base field (like your field: %s) do not have an alias - you should look them up via an "
      + "AvroRecordDecoder", aliasName);
    Map<String, List<String>> names =
      metric.getMetadata().getCanonicalNamesToAliases();

    String cname = null;
    for (Map.Entry<String, List<String>> nameToAliases : names.entrySet()) {
      if (nameToAliases.getValue().contains(aliasName)) {
        cname = nameToAliases.getKey();
        break;
      }
    }

    return cname;
  }
}