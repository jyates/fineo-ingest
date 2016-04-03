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
import static org.junit.Assert.fail;

/**
 * Verify that the {@link FirehoseRecordWriter} and {@link FirehoseRecordReader} work together
 */
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
    // read back in the record
    byte[] raw = out.toByteArray();
    FirehoseRecordReader<GenericRecord> reader = FirehoseRecordReader.create(ByteBuffer.wrap(raw));
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
   * Ensure that we read from the correct position in the byte buffer, which in this test should
   * cause a failure in the reading
   *
   * @throws Exception
   */
  @Test
  public void failReadMalformedByteBuffer() throws Exception {
    GenericRecord record = SchemaTestUtils.createRandomRecord();
    FirehoseRecordWriter writer = new FirehoseRecordWriter();
    ByteBuffer buff = writer.write(record);
    ByteBuffer malformed = ByteBufferUtils.skipFirstByteCopy(buff);
    FirehoseRecordReader reader = FirehoseRecordReader.create(malformed);
    try {
      reader.next();
      fail("Should not have been able to parse a record!");
    }catch (IOException e){
      // expected
    }
  }
}