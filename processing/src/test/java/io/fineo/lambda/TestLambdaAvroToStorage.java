package io.fineo.lambda;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.collect.Lists;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.avro.file.ByteBufferUtils;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestLambdaAvroToStorage {

  @Test
  public void testSingleRecordWrite() throws Exception {
    GenericRecord record = SchemaTestUtils.createRandomRecord();
    FirehoseRecordWriter writer = FirehoseRecordWriter.create();
    ByteBuffer buff = writer.write(record);
    KinesisEvent event = LambdaTestUtils.getKinesisEvent(buff);

    // setup mocks
    FirehoseBatchWriter records = Mockito.mock(FirehoseBatchWriter.class);
    AvroToDynamoWriter dynamo = Mockito.mock(AvroToDynamoWriter.class);
    MultiWriteFailures failures = new MultiWriteFailures(Lists.newArrayList());
    Mockito.when(dynamo.flush()).thenReturn(failures);

    // actually do the write
    LambdaAvroToStorage storage = new LambdaAvroToStorage();
    storage.setupForTesting(props(), dynamo, records, null, null);
    storage.handleEventInternal(event);

    // verify that we wrote the record the proper places
    verifyBufferAddedAndFlushed(records, buff);
    Mockito.verify(dynamo).write(Mockito.any());
    Mockito.verify(dynamo).flush();
  }

  @Test
  public void testMalformedAvroRecord() throws Exception {
    GenericRecord record = SchemaTestUtils.createRandomRecord();
    FirehoseRecordWriter writer = FirehoseRecordWriter.create();
    ByteBuffer buff = writer.write(record);
    // create a malformed record, missing the first byte
    ByteBuffer malformed = ByteBufferUtils.skipFirstByteCopy(buff);
    KinesisEvent event = LambdaTestUtils.getKinesisEvent(malformed);

    // setup mocks
    OutputFirehoseManager manager = new OutputFirehoseManager().withProcessingErrors();
    AvroToDynamoWriter dynamo = Mockito.mock(AvroToDynamoWriter.class);
    Mockito.when(dynamo.flush()).thenReturn(new MultiWriteFailures<>(new ArrayList<>(0)));

    List<ByteBuffer> errors = manager.listenForProcesssingErrors();

    // actually do the write
    LambdaAvroToStorage storage = new LambdaAvroToStorage();
    storage
      .setupForTesting(props(), dynamo, manager.archive(), manager.process(), manager.commit());
    storage.handleEventInternal(event);

    // verify that we wrote the record the proper places
    verifyBufferAddedAndFlushed(manager.archive(), malformed);
    manager.verifyErrors();

    // verify the record we failed
    assertEquals(Lists.newArrayList(malformed), errors);
  }

  /**
   * Test that we send the data to the 'processing error' writer if we cannot write to dynamo
   *
   * @throws Exception
   */
  @Test
  public void testFailedDynamoWrite() throws Exception {
    GenericRecord record = SchemaTestUtils.createRandomRecord();
    FirehoseRecordWriter writer = FirehoseRecordWriter.create();
    ByteBuffer buff = writer.write(record);
    KinesisEvent event = LambdaTestUtils.getKinesisEvent(buff);

    // setup mocks
    OutputFirehoseManager manager = new OutputFirehoseManager().withCommitErrors();
    AvroToDynamoWriter dynamo = Mockito.mock(AvroToDynamoWriter.class);

    MultiWriteFailures<GenericRecord> failures = new MultiWriteFailures<>(Lists.newArrayList(new
      AwsAsyncRequest<>(record, AmazonWebServiceRequest.NOOP)));
    Mockito.when(dynamo.flush()).thenReturn(failures);

    List<ByteBuffer> errors = manager.listenForCommitErrors();

    // actually do the write
    LambdaAvroToStorage storage = new LambdaAvroToStorage();
    storage
      .setupForTesting(props(), dynamo, manager.archive(), manager.process(), manager.commit());
    storage.handleEventInternal(event);

    // verify that we wrote the record the proper places
    verifyBufferAddedAndFlushed(manager.archive(), buff);
    Mockito.verify(dynamo).write(Mockito.any());
    Mockito.verify(dynamo).flush();
    manager.verifyErrors();

    // verify the record we failed
    assertEquals(1, errors.size());
    FirehoseRecordReader<GenericRecord> recordReader = FirehoseRecordReader.create(errors.get(0));
    assertEquals(record, recordReader.next());
    assertNull(recordReader.next());
  }

  private void verifyBufferAddedAndFlushed(FirehoseBatchWriter writer, ByteBuffer buff)
    throws IOException {
    Mockito.verify(writer).addToBatch(buff);
    Mockito.verify(writer).flush();
  }

  private LambdaClientProperties props() {
    return LambdaClientProperties.createForTesting(new Properties(), null);
  }
}