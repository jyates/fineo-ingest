package org.apache.avro.file;

import com.google.common.collect.Lists;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Record;
import io.fineo.schema.avro.AvroSchemaBridge;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class TestFirehoseReadWrite {

  private static final Log LOG = LogFactory.getLog(TestFirehoseReadWrite.class);

  @Test
  public void testReadWriteSingleRecord() throws Exception {
    writeAndVerifyRecordsAndCodec(createRandomRecord());
  }

  @Test
  public void testTwoRecordsSerDe() throws Exception {
    writeAndVerifyRecordsAndCodec(createRandomRecord(), createRandomRecord());
  }

  private void writeAndVerifyRecordsAndCodec(GenericRecord... records)
    throws IOException {
    FirehoseRecordWriter writer = new FirehoseRecordWriter();
    writeAndVerifyRecords(writer, records);

    writer = new FirehoseRecordWriter();
    writer.setCodec(CodecFactory.bzip2Codec());
    writeAndVerifyRecords(writer, records);
  }

  /**
   * Actual work of writing, reading and verifying that the records match
   *
   * @param writer
   * @param records
   * @throws IOException
   */
  private void writeAndVerifyRecords(FirehoseRecordWriter writer, GenericRecord... records)
    throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (GenericRecord record : records) {
      LOG.info("Wrote record: " + record + ", schema: " + record.getSchema());
      ByteBuffer bb = writer.write(record);
      out.write(bb.array(), bb.arrayOffset(), bb.limit());
    }

    out.close();
    byte[] raw = out.toByteArray();
    // read back in the record
    SeekableByteArrayInput is = new SeekableByteArrayInput(raw);
    FirehoseRecordReader<GenericRecord> reader = new FirehoseRecordReader<>(is);
    List<GenericRecord> recordList = Lists.newArrayList(records);
    LOG.info("Starting with expected records: " + recordList);
    for (int i = 0; i < records.length; i++) {
      GenericRecord record1 = reader.next();
      LOG.info("Got record: " + record1);
      assertTrue("remaining records: " + recordList + "\nmissing: " + record1,
        recordList.remove(record1));
    }
  }

  /**
   * Create a randomish schema using our schema generation utilities
   *
   * @return a record with a unique schema
   * @throws IOException
   */
  private GenericRecord createRandomRecord() throws Exception {
    return createRandomRecord(1).get(0);
  }

  private List<GenericRecord> createRandomRecord(int count) throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    // create a semi-random schema
    String orgId = "orgId";
    String metricType = "metricType";
    int fieldCount = new Random().nextInt(10);
    LOG.info("Random field count: " + fieldCount);
    String[] fieldNames =
      IntStream.range(0, fieldCount)
               .mapToObj(index -> "a" + index)
               .collect(Collectors.toList())
               .toArray(new String[0]);
    SchemaTestUtils.addNewOrg(store, orgId, metricType, fieldNames);

    // create random records with the above schema
    AvroSchemaBridge bridge = AvroSchemaBridge.create(store, orgId, metricType);
    List<GenericRecord> records = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      Map<String, Object> fields = SchemaTestUtils.getBaseFields(orgId, metricType);
      Record r = new MapRecord(fields);
      for (int j = 0; j < fieldCount; j++) {
        fields.put("a" + j, true);
      }
      records.add(bridge.encode(r));
    }
    return records;
  }
}