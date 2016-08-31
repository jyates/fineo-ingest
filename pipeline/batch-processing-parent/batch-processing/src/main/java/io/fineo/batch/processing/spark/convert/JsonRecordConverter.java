package io.fineo.batch.processing.spark.convert;

import io.fineo.schema.avro.AvroSchemaEncoder;

import java.util.Map;
import java.util.Properties;

/**
 * Convert raw json events into avro typed records. This makes a large amount of database calls,
 * so you should probably checkpoint the RDD after complete to ensure we don't do it multiple times.
 */
public class JsonRecordConverter extends RecordConverter<Map<String, Object>>{

  private final String orgId;

  public JsonRecordConverter(String orgId, Properties props) {
    super(props);
    this.orgId = orgId;
  }

  @Override
  protected Map<String, Object> transform(Map<String, Object> obj) {
    obj.put(AvroSchemaEncoder.ORG_ID_KEY, orgId);
    return obj;
  }

}
