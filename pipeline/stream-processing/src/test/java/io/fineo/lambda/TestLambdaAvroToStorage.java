package io.fineo.lambda;

import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import io.fineo.lambda.avro.ByteBufferUtils;
import io.fineo.lambda.avro.FirehoseRecordReader;
import io.fineo.lambda.avro.FirehoseRecordWriter;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.configure.MockOnNullInstanceModule;
import io.fineo.lambda.configure.NullableNamedInstanceModule;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import io.fineo.lambda.handle.KinesisHandler;
import io.fineo.lambda.handle.LambdaWrapper;
import io.fineo.lambda.handle.MalformedEventToJson;
import io.fineo.lambda.handle.staged.AvroToStorageHandler;
import io.fineo.lambda.handle.staged.AvroToStorageWrapper;
import io.fineo.lambda.util.LambdaTestUtils;
import io.fineo.schema.store.SchemaTestUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
    MultiWriteFailures failures = new MultiWriteFailures(newArrayList());
    Mockito.when(dynamo.flush()).thenReturn(failures);

    // actually do the write
    LambdaWrapper<KinesisEvent, AvroToStorageHandler> storage = getLambda(records, dynamo);
    storage.handleEvent(event);

    // verify that we wrote the record the proper places
    verifyBufferAddedAndFlushed(records, buff);
    Mockito.verify(dynamo).write(Mockito.any());
    Mockito.verify(dynamo).flush();
  }

  private LambdaWrapper<KinesisEvent, AvroToStorageHandler> getLambda(FirehoseBatchWriter records,
    AvroToDynamoWriter dynamo) {
    return getLambda(dynamo, records, null, null);
  }

  private LambdaWrapper<KinesisEvent, AvroToStorageHandler> getLambda(
    AvroToDynamoWriter dynamo, FirehoseBatchWriter records, FirehoseBatchWriter malformed,
    FirehoseBatchWriter error) {
    List<Module> modules = HandlerSetupUtils.getBasicTestModules(records, malformed, error);
    modules.add(new MockOnNullInstanceModule<>(dynamo, AvroToDynamoWriter.class));
    return new AvroToStorageWrapper(modules);
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
    LambdaWrapper<KinesisEvent, AvroToStorageHandler> storage =
      getLambda(dynamo, manager.archive(), manager.process(), manager.commit());
    storage.handleEvent(event);

    // verify that we wrote the record the proper places
    verifyBufferAddedAndFlushed(manager.archive(), malformed);
    manager.verifyErrors();

    // verify the record we failed
    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> maps = errors.stream().map(bytes -> {
      byte[] raw = new byte[bytes.remaining()];
      bytes.get(raw);
      return new String(raw);
    }).map(text -> {
      try {
        return (Map<String, Object>) mapper.readValue(text, Map.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    assertEquals("Wrong number of results! Got error rows: " + maps, maps.size(), errors.size());
    // verify the pertinent part of each map
    for (int i = 0; i < maps.size(); i++) {
      Map<String, Object> actual = maps.get(i);
      assertEquals(KinesisHandler.FINEO_INTERNAL_ERROR_API_KEY, actual.get("apikey"));
      assertTrue(((String) actual.get("message")).startsWith("First byte does not match expected "
                                                             + "magic"));
      //      assertEquals(1, ((List) actual.get("causes")).size());
    }
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

    MultiWriteFailures<GenericRecord, UpdateItemRequest> failures =
      new MultiWriteFailures<>(
        newArrayList(new AwsAsyncRequest<>(record, new UpdateItemRequest())));
    Mockito.when(dynamo.flush()).thenReturn(failures);

    List<ByteBuffer> errors = manager.listenForCommitErrors();

    // actually do the write
    LambdaWrapper<KinesisEvent, AvroToStorageHandler> storage =
      getLambda(dynamo, manager.archive(), manager.process(), manager.commit());
    storage.handleEvent(event);

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
}
