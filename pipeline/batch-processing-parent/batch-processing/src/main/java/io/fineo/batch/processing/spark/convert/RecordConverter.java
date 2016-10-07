package io.fineo.batch.processing.spark.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.internal.customer.Malformed;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.schema.store.AvroSchemaProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Convert raw json events into avro typed records. This makes a large amount of database calls,
 * so you should probably checkpoint the RDD after complete to ensure we don't do it multiple times.
 */
public class RecordConverter
  implements PairFunction<Row, ReadResult, GenericRecord>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RecordConverter.class);
  private transient ObjectMapper mapper;
  private final BatchOptions options;
  private transient RawJsonToRecordHandler handler;
  private LocalQueueKinesisProducer queue;

  private final String orgId;

  public RecordConverter(String orgId, BatchOptions options) {
    this.options = options;
    this.orgId = orgId;
  }

  @Override
  public Tuple2<ReadResult, GenericRecord> call(Row obj) throws Exception {
    RawJsonToRecordHandler handler = getHandler();
    try {
      handler.handle(transform(obj));
      return new Tuple2<>(new ReadResult(ReadResult.Outcome.SUCCESS, orgId),
        queue.getRecords().remove());
    } catch (Exception e) {
      LOG.error("Found malformed record: {}", obj, e);
      Malformed mal = Malformed.newBuilder()
                               .setRecordContent(ByteBuffer.wrap(rowBackToJson(obj).getBytes()))
                               .setOrg(orgId)
                               .setMessage(e.getMessage())
                               .build();
      return new Tuple2<>(new ReadResult(ReadResult.Outcome.FAILURE, orgId), mal);
    }
  }

  private String rowBackToJson(Row row) throws JsonProcessingException {
    Map<String, Object> event = new HashMap<>();
    StructType schema = row.schema();
    for (String field : schema.fieldNames()) {
      event.put(field, row.getAs(field));
    }

    return getMapper().writeValueAsString(event);
  }

  protected Map<String, Object> transform(Row row) {
    Map<String, Object> values = new HashMap<>(row.size());
    values.put(AvroSchemaProperties.ORG_ID_KEY, orgId);
    StructType schema = row.schema();
    for (String name : schema.fieldNames()) {
      values.put(name, row.get(row.fieldIndex(name)));
    }
    return values;
  }

  private RawJsonToRecordHandler getHandler() {
    if (this.handler == null) {
      this.queue = new LocalQueueKinesisProducer();
      handler = options.getRawJsonToRecordHandler(queue);
    }
    return handler;
  }

  private ObjectMapper getMapper() {
    if (this.mapper == null) {
      this.mapper = new ObjectMapper();
    }
    return this.mapper;
  }
}
