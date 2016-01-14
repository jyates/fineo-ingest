package io.fineo.lambda.storage;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.google.common.collect.Lists;
import io.fineo.lambda.avro.FirehoseBatchWriter;
import io.fineo.lambda.LambdaTestUtils;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.file.FirehoseRecordWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
    FirehoseBatchWriter error = Mockito.mock(FirehoseBatchWriter.class);
    AvroToDynamoWriter dynamo = Mockito.mock(AvroToDynamoWriter.class);
    MultiWriteFailures failures = new MultiWriteFailures(Lists.newArrayList());
    Mockito.when(dynamo.flush()).thenReturn(failures);

    // actually do the write
    LambdaAvroToStorage storage = new LambdaAvroToStorage();
    storage.setupForTesting(records, error, dynamo);
    storage.handleEventInternal(event);

    // verify that we wrote the record the proper places
    verifyBufferAddedAndFlushed(records, buff);
    Mockito.verify(dynamo).write(Mockito.any());
    Mockito.verify(dynamo).flush();
    Mockito.verifyZeroInteractions(error);
  }

  @Test
  public void testFailedWrite() throws Exception {
    GenericRecord record = SchemaTestUtils.createRandomRecord();
    FirehoseRecordWriter writer = FirehoseRecordWriter.create();
    ByteBuffer buff = writer.write(record);
    KinesisEvent event = LambdaTestUtils.getKinesisEvent(buff);

    // setup mocks
    FirehoseBatchWriter records = Mockito.mock(FirehoseBatchWriter.class);
    FirehoseBatchWriter error = Mockito.mock(FirehoseBatchWriter.class);
    AvroToDynamoWriter dynamo = Mockito.mock(AvroToDynamoWriter.class);

    AvroToDynamoWriter.UpdateItemHandler handler = Mockito.mock(AvroToDynamoWriter
      .UpdateItemHandler.class);
    Mockito.when(handler.getBaseRecord()).thenReturn(record);
    MultiWriteFailures failures = new MultiWriteFailures(Lists.newArrayList(handler));
    Mockito.when(dynamo.flush()).thenReturn(failures);

    List<ByteBuffer> errors = new ArrayList<>();
    Mockito.doAnswer(invocation -> {
      errors.add((ByteBuffer) invocation.getArguments()[0]);
      return null;
    }).when(error).addToBatch(Mockito.any(ByteBuffer.class));

    // actually do the write
    LambdaAvroToStorage storage = new LambdaAvroToStorage();
    storage.setupForTesting(records, error, dynamo);
    storage.handleEventInternal(event);

    // verify that we wrote the record the proper places
    verifyBufferAddedAndFlushed(records, buff);
    Mockito.verify(dynamo).write(Mockito.any());
    Mockito.verify(dynamo).flush();
    Mockito.verify(error).addToBatch(Mockito.any());
    Mockito.verify(error).flush();
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
}