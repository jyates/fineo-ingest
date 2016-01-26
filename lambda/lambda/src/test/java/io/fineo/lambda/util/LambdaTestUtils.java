package io.fineo.lambda.util;


import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.jr.ob.JSON;
import com.google.common.collect.Lists;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

public class LambdaTestUtils {

  private static final Log LOG = LogFactory.getLog(LambdaTestUtils.class);

  private LambdaTestUtils() {
  }

  public static KinesisEvent getKinesisEvent(ByteBuffer data) {
    KinesisEvent event = Mockito.mock(KinesisEvent.class);
    KinesisEvent.KinesisEventRecord kinesisRecord = new KinesisEvent.KinesisEventRecord();
    KinesisEvent.Record record = new KinesisEvent.Record();
    record.setData(data);
    kinesisRecord.setKinesis(record);
    Mockito.when(event.getRecords()).thenReturn(Lists.newArrayList(kinesisRecord));
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
    Map[] records = new Map[count];
    for (int i = 0; i < count; i++) {
      String uuid = UUID.randomUUID().toString();
      LOG.debug("Using UUID - " + uuid);
      Map<String, Object> map =
        SchemaTestUtils.getBaseFields("org" + i + "_" + uuid, "mt" + i + "_" + uuid);
      for (int j = 0; j < fieldCount; j++) {
        map.put("a" + j, true);
      }
      records[i] = map;
    }
    return (Map<String, Object>[]) records;
  }
}