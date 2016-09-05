package io.fineo.batch.processing.spark.convert;

import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.schema.store.AvroSchemaProperties;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

/**
 * Convert a dataframe Row into a json-like map[String, Object] and then passes it to the
 * {@link RecordConverter}
 *
 * @see RecordConverter
 */
public class RowRecordConverter extends RecordConverter<Row> {

  private final String orgId;

  public RowRecordConverter(String orgId, BatchOptions options) {
    super(options);
    this.orgId = orgId;
  }

  @Override
  protected Map<String, Object> transform(Row row) {
    Map<String, Object> values = new HashMap<>(row.size());
    values.put(AvroSchemaProperties.ORG_ID_KEY, orgId);
    StructType schema = row.schema();
    for (String name : schema.fieldNames()) {
      values.put(name, row.get(row.fieldIndex(name)));
    }
    return values;
  }
}
