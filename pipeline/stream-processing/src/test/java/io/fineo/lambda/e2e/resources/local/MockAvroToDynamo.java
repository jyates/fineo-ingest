package io.fineo.lambda.e2e.resources.local;

import com.google.inject.Injector;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.resources.manager.collector.OutputCollector;
import io.fineo.lambda.e2e.resources.manager.IDynamoResource;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.fineo.lambda.e2e.validation.util.ValidationUtils.verifyRecordMatchesJson;
import static org.junit.Assert.assertEquals;

/**
 * Wrapper around mock access to {@link AvroToDynamoWriter}
 */
public class MockAvroToDynamo implements IDynamoResource {

  private static final Logger LOG = LoggerFactory.getLogger(MockAvroToDynamo.class);

  private final List<GenericRecord> dynamoWrites = new ArrayList<>();
  private AvroToDynamoWriter dynamo;
  private SchemaStore store;

  @Override
  public void init(Injector injector) {
    this.store = injector.getInstance(SchemaStore.class);
    this.dynamo = Mockito.mock(AvroToDynamoWriter.class);
    Mockito.doAnswer(invocationOnMock -> {
      GenericRecord record = (GenericRecord) invocationOnMock.getArguments()[0];
      LOG.info("Adding record: " + record);
      dynamoWrites.add(record);
      return null;
    }).when(dynamo).write(Mockito.any(GenericRecord.class));
    Mockito.when(dynamo.flush()).thenReturn(new MultiWriteFailures(Collections.emptyList()));
  }

  public AvroToDynamoWriter getWriter() {
    return dynamo;
  }

  @Override
  public void verify(RecordMetadata metadata, Map<String, Object> json) {
    assertEquals("Got wrong number of writes: " + dynamoWrites, 1, dynamoWrites.size());
    verifyRecordMatchesJson(store, json, dynamoWrites.get(0));
  }

  @Override
  public void copyStoreTables(OutputCollector dynamo) throws IOException {
    try (OutputStream out = dynamo.get("writes")) {
      int i = 0;
      for (GenericRecord record : dynamoWrites) {
        out.write(((i++) + ": " + record.toString()).getBytes());
      }
    }
  }


  @Override
  public void cleanup(FutureWaiter waiter) {
    this.dynamoWrites.clear();
  }
}
