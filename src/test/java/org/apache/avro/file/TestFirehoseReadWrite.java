package org.apache.avro.file;

import com.google.common.collect.Lists;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestFirehoseReadWrite {

  private static final Log LOG = LogFactory.getLog(TestFirehoseReadWrite.class);

  @Test
  public void testReadWriteSingleRecord() throws Exception {
    writeAndVerifyRecordsAndCodec(SchemaTestUtils.createRandomRecord());
  }

  @Test
  public void testTwoRecordsSerDe() throws Exception {
    writeAndVerifyRecordsAndCodec(
      SchemaTestUtils.createRandomRecord(), SchemaTestUtils.createRandomRecord());
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

}