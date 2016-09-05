package io.fineo.lambda.avro;

import io.fineo.schema.store.SchemaTestUtils;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verify that the {@link FirehoseRecordWriter} and {@link FirehoseRecordReader} work together
 */
public class TestFirehoseReadWrite {

  private static final Log LOG = LogFactory.getLog(TestFirehoseReadWrite.class);

  @Test
  public void testReadWriteSingleRecord() throws Exception {
    writeAndVerifyRecordsAndCodec(
      SchemaTestUtils.createRandomRecord("orgId", "metricType", System.currentTimeMillis(), 1, 100)
                     .get(0));
  }

  @Test
  public void testTwoRecordsSerDe() throws Exception {
    writeAndVerifyRecordsAndCodec(
      SchemaTestUtils.createRandomRecord(), SchemaTestUtils.createRandomRecord());
  }

  @Test
  public void testReadSequentialRecords() throws Exception {
    FirehoseRecordWriter writer = FirehoseRecordWriter.create();
    GenericRecord record1 = SchemaTestUtils.createRandomRecord();
    byte[] data = write(writer, record1).getKey();
    byte[] data2 = write(writer, record1).getKey();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    bos.write(data);
    bos.write(data2);
    bos.close();

    byte[] raw = bos.toByteArray();
    FirehoseRecordReader<GenericRecord> reader = FirehoseRecordReader.create(ByteBuffer.wrap(raw));
    assertEquals(record1,reader.next());
    assertEquals(record1,reader.next());
    assertNull(reader.next());

    reader = FirehoseRecordReader.create(new SeekableByteArrayInput(raw));
    assertEquals(record1,reader.next());
    assertEquals(record1,reader.next());
    assertNull(reader.next());
  }

  private void writeAndVerifyRecordsAndCodec(GenericRecord... records)
    throws IOException {
    // null codec and deflator
    FirehoseRecordWriter writer = new FirehoseRecordWriter();
    writeAndVerifyRecords(writer, records);

    // defaults
    writer = FirehoseRecordWriter.create();
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
    for (GenericRecord record : records) {
      String out = record.toString();
      LOG.info("Incoming size: " + out.getBytes().length);
    }
    byte[] raw = write(writer, records).getLeft();

    // read back in the record
    FirehoseRecordReader<GenericRecord> reader = FirehoseRecordReader.create(ByteBuffer.wrap(raw));
    List<GenericRecord> recordList = newArrayList(records);
    LOG.info("Starting with expected records: " + recordList);
    for (int i = 0; i < records.length; i++) {
      GenericRecord record1 = reader.next();
      LOG.info("Got record: " + record1);
      assertTrue("remaining records: " + recordList + "\nmissing: " + record1,
        recordList.remove(record1));
    }
  }

  private Pair<String, CodecFactory> pair(String bzip, CodecFactory codecFactory) {
    return new ImmutablePair<>(bzip, codecFactory);
  }

  private Pair<byte[], Long> write(FirehoseRecordWriter writer,
    GenericRecord... records)
    throws IOException {
    long start = System.nanoTime();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (GenericRecord record : records) {
      ByteBuffer bb = writer.write(record);
      out.write(bb.array(), bb.arrayOffset(), bb.limit());
    }
    out.close();
    long stop = System.nanoTime();
    return new ImmutablePair<>(out.toByteArray(), stop - start);
  }

  /**
   * Ensure that we read from the correct position in the byte buffer, which in this test should
   * cause a failure in the reading
   *
   * @throws Exception
   */
  @Test(expected = IllegalArgumentException.class)
  public void failReadMalformedByteBuffer() throws Exception {
    GenericRecord record = SchemaTestUtils.createRandomRecord();
    FirehoseRecordWriter writer = new FirehoseRecordWriter();
    ByteBuffer buff = writer.write(record);
    ByteBuffer malformed = ByteBufferUtils.skipFirstByteCopy(buff);
    FirehoseRecordReader reader = FirehoseRecordReader.create(malformed);
    reader.next();
  }

  private static Function<OutputStream, OutputStream> GZIP = out -> {
    try {
      return new GZIPOutputStream(out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  };
}
