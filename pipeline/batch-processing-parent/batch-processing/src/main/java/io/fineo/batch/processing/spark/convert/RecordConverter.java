package io.fineo.batch.processing.spark.convert;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.internal.customer.Malformed;
import io.fineo.lambda.handle.TenantBoundFineoException;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.store.AvroSchemaProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import scala.Tuple2;

import static io.fineo.lambda.handle.KinesisHandler.FINEO_INTERNAL_ERROR_API_KEY;

/**
 * Convert raw json events into avro typed records. This makes a large amount of database calls,
 * so you should probably checkpoint the RDD after complete to ensure we don't do it multiple times.
 */
public class RecordConverter
  implements PairFunction<Row, ReadResult, GenericRecord>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RecordConverter.class);

  private transient ObjectMapper mapper;
  private transient RawJsonToRecordHandler handler;

  private final BatchOptions options;
  private LocalQueueKinesisProducer queue;
  private transient Clock clock;
  private final String orgId;

  public RecordConverter(String orgId, BatchOptions options) {
    this.options = options;
    this.orgId = orgId;
  }

  @Override
  public Tuple2<ReadResult, GenericRecord> call(Row obj) throws Exception {
    RawJsonToRecordHandler handler = getHandler();
    Malformed mal;
    try {
      handler.handle(transform(obj));
      return new Tuple2<>(new ReadResult(ReadResult.Outcome.SUCCESS, orgId),
        queue.getRecords().remove());
    } catch (TenantBoundFineoException e) {
      mal = addErrorRecord(obj, e.getApikey(), e.getWriteTime(), e);
    } catch (RuntimeException e) {
      mal = addErrorRecord(obj, e);
    }
    LOG.error("Found malformed record: {}", obj);
    return new Tuple2<>(new ReadResult(ReadResult.Outcome.FAILURE, orgId), mal);
  }


  private Malformed addErrorRecord(Row rec, RuntimeException e)
    throws IOException {
    return addErrorRecord(rec, null, -1, e);
  }

  private Malformed addErrorRecord(Row rec, String apiKey, long
    writeTime, Exception thrown)
    throws IOException {
    if (apiKey == null) {
      apiKey = FINEO_INTERNAL_ERROR_API_KEY;
    }
    if (writeTime < 0) {
      writeTime = getClock().millis();
    }
    ByteBuffer data = ByteBuffer.wrap(rowBackToJson(rec).getBytes());
    return new Malformed(apiKey, thrown.getMessage(), data, writeTime/*, toThrown(thrown)*/);
  }

  private Clock getClock() {
    if(this.clock == null){
      this.clock = Clock.systemUTC();
    }
    return this.clock;
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

  private RawJsonToRecordHandler getHandler() throws IOException, OldSchemaException {
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
