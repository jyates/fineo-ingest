package io.fineo.batch.processing.spark.convert;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Convert a dataframe Row into a json-like map[String, Object] and then passes it to the
 * {@link RecordConverter}
 *
 * @see RecordConverter
 */
public class RowRecordConverter extends RecordConverter<Row> {

  public RowRecordConverter(Properties props) {
    super(props);
  }

  @Override
  protected Map<String, Object> transform(Row row) {
    Map<String, Object> values = new HashMap<>(row.size());
    StructType schema = row.schema();
    for (String name : schema.fieldNames()) {
      values.put(name, row.get(row.fieldIndex(name)));
    }
    return values;
  }
}
