package io.fineo.lambda.e2e.resources.dynamo;

import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.fineo.lambda.e2e.EndToEndTestRunner.verifyRecordMatchesJson;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class MockAvroToDynamo {

  private final AvroToDynamoWriter dynamo;
  private final List<GenericRecord> dynamoWrites = new ArrayList<>();
  private final SchemaStore store;

  public MockAvroToDynamo(SchemaStore store){
    this.store = store;
    this.dynamo = Mockito.mock(AvroToDynamoWriter.class);
    Mockito.doAnswer(invocationOnMock -> {
      dynamoWrites.add((GenericRecord) invocationOnMock.getArguments()[0]);
      return null;
    }).when(dynamo).write(Mockito.any(GenericRecord.class));
    Mockito.when(dynamo.flush()).thenReturn(new MultiWriteFailures(Collections.emptyList()));
  }

  public void cleanup() {
    this.dynamoWrites.clear();
  }

  public void verifyWrites(RecordMetadata metadata, Map<String, Object> json) {
    assertEquals(1, dynamoWrites.size());
    verifyRecordMatchesJson(store, json, dynamoWrites.get(0));
  }

  public AvroToDynamoWriter getWriter() {
    return dynamo;
  }
}
