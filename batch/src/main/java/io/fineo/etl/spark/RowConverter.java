package io.fineo.etl.spark;

import io.fineo.internal.customer.BaseFields;
import io.fineo.schema.avro.RecordMetadata;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;

import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Function to convert GenericRecords into {@link Row Rows} based on the schema
 */
public class RowConverter implements Function<GenericRecord, Row>, Serializable {

  private final String schemaString;
  private final String orgId;
  private final String metricName;
  private final Map<String, List<String>> canonicalNamesToAliases;
  private Schema.Parser parser;

  public RowConverter(String schemaString, Map<String, List<String>> canonicalNamesToAliases,
    String orgId, String metricCanonicalName) {
    this.schemaString = schemaString;
    this.canonicalNamesToAliases = canonicalNamesToAliases;
    this.orgId = orgId;
    this.metricName = metricCanonicalName;
  }

  @Override
  public Row call(GenericRecord record) throws Exception {
    if (this.parser == null) {
      this.parser = new Schema.Parser();
    }

    Schema schema = parser.parse(schemaString);
    RecordMetadata metadata = RecordMetadata.get(record);
    BaseFields base = metadata.getBaseFields();
    List<Object> fields = new ArrayList<>();
    // populate the partitions
    fields.add(orgId);
    fields.add(metricName);
    fields.add(new Date(base.getTimestamp()));
    fields.add(base.getTimestamp());

    // populate the other fields, as we have them
    SparkETL.streamSchemaWithoutBaseFields(schema)
            .map(field -> field.name())
            .forEach(name -> {
              Object value = record.get(name);
              // we don't know about that schema type, but maybe the schema has been updated
              // and have it as an alias
              if (value == null) {
                List<String> aliases = this.canonicalNamesToAliases.get(name);
                for (String alias : aliases) {
                  value = base.getUnknownFields().get(alias);
                  if (value != null) {
                    break;
                  }
                }
              }
              fields.add(value);
            });

    return RowFactory.create(fields.toArray());
  }
}
