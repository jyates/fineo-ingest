package io.fineo.lambda.util;


import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.collect.Lists;
import io.fineo.lambda.avro.FirehoseRecordReader;
import io.fineo.schema.store.SchemaTestUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

public class LambdaTestUtils {

  private static final Log LOG = LogFactory.getLog(LambdaTestUtils.class);

  private LambdaTestUtils() {
  }

  public static KinesisEvent getKinesisEvent(ByteBuffer data) {
    return getKinesisEvent(Lists.newArrayList(data));
  }

  public static KinesisEvent getKinesisEvent(List<ByteBuffer> data) {
    KinesisEvent event = Mockito.mock(KinesisEvent.class);
    List<KinesisEvent.KinesisEventRecord> records = new ArrayList<>();
    for (ByteBuffer buff : data) {
      KinesisEvent.KinesisEventRecord kinesisRecord = new KinesisEvent.KinesisEventRecord();
      KinesisEvent.Record record = new KinesisEvent.Record();
      record.setData(buff);
      kinesisRecord.setKinesis(record);
      records.add(kinesisRecord);
    }
    Mockito.when(event.getRecords()).thenReturn(records);

    return event;
  }

  public static byte[] asBytes(Map<String, Object> fields) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JSON.std.write(fields, baos);
    baos.close();
    return baos.toByteArray();
  }

  public static KinesisEvent getKinesisEvent(Map<String, Object> fields) throws IOException {
    return getKinesisEvent(ByteBuffer.wrap(asBytes(fields)));
  }

  public static Map<String, Object>[] createRecords(int count) {
    return createRecords(count, 0);
  }

  public static Map<String, Object>[] createRecords(int count, int fieldCount) {
    BiFunction<Integer, String, Object> valueGen = new BiFunction<Integer, String, Object>() {
      private boolean previous = false;

      @Override
      public Object apply(Integer integer, String s) {
        boolean next = previous;
        previous = !previous;
        return next;
      }
    };
    Map<String, Object>[] ret = new Map[0];
    for (int i = 0; i < count; i++) {
      ret = (Map<String, Object>[]) ArrayUtils.addAll(ret, createRecordsForSingleTenant(1, i,
        fieldCount, valueGen));
    }
    return ret;
  }

  public static Map<String, Object>[] createRecordsForSingleTenant(int recordCount, int fieldCount,
    BiFunction<Integer, String, Object> fieldGenerator){
    return createRecordsForSingleTenant(recordCount, fieldCount, 0, fieldGenerator);
  }
  public static Map<String, Object>[] createRecordsForSingleTenant(int recordCount, int
    fieldCount, int tenantId, BiFunction<Integer, String, Object> fieldGenerator) {
    Map[] records = new Map[recordCount];
    String uuid = UUID.randomUUID().toString();
    LOG.debug("Using UUID - " + uuid);
    long ts = System.currentTimeMillis();
    for (int i = 0; i < recordCount; i++) {
      Map<String, Object> map =
        SchemaTestUtils.getBaseFields("org" + "_" + tenantId+"_"+uuid, "mt" + "_" + uuid, ts++);
      // set the field values
      for (int j = 0; j < fieldCount; j++) {
        String name = "a" + j;
        map.put(name, fieldGenerator.apply(i, name));
      }
      records[i] = map;
    }
    return (Map<String, Object>[]) records;
  }

  public static List<GenericRecord> readRecords(ByteBuffer data) {
    try {
      List<GenericRecord> records = new ArrayList<>();
      FirehoseRecordReader<GenericRecord> recordReader =
        FirehoseRecordReader.create(data);
      GenericRecord record = recordReader.next();
      if (record != null) {
        records.add(record);
      }
      return records;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
