package org.apache.avro.file;

import com.google.common.collect.Lists;
import io.fineo.lambda.avro.FirehoseWriter;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.store.SchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

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
    FirehoseWriter writer = new FirehoseWriter();
    writeAndVerifyRecords(writer, records);

    writer = new FirehoseWriter();
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
  private void writeAndVerifyRecords(FirehoseWriter writer, GenericRecord... records)
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
    FirehoseReader<GenericRecord> reader = new FirehoseReader<>(is);
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
  private GenericRecord createRandomRecord() throws IOException {
    return createRandomRecord(1).get(0);
  }

  private List<GenericRecord> createRandomRecord(int count) throws IOException {
    List<GenericRecord> records = new ArrayList<>(count);
    AvroSchemaInstanceBuilder builder = new AvroSchemaInstanceBuilder();
    // create a randomish name
    String name = UUID.randomUUID().toString();
    //String name = nameIterator.next();
    LOG.info("UUID: " + name);
    name = "a" + String.format("%x", new BigInteger(1, name.getBytes()));
    LOG.info("Record name: " + name);
    builder.withName(name).withNamespace("ns");
    int fieldCount = new Random().nextInt(10);
    //int fieldCount = countIterator.next();
    LOG.info("Random field count: " + fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      builder.newField().name("a" + i).type("boolean").done();
    }
    Schema schema = builder.build();

    // create a record with the schema
    for (int i = 0; i < count; i++) {
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema)
        .set(SchemaBuilder.UNKNOWN_KEYS_FIELD, new HashMap<>());
      for (int j = 0; j < fieldCount; j++) {
        recordBuilder.set("a" + j, true);
      }
      records.add(recordBuilder.build());
    }
    return records;
  }
}
