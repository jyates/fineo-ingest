package io.fineo.lambda.e2e.resources.manager;

import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.resources.manager.collector.OutputCollector;
import io.fineo.schema.avro.RecordMetadata;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public interface IDynamoResource extends IResource {

  public AvroToDynamoWriter getWriter();

  void verify(RecordMetadata metadata, Map<String, Object> json);

  void copyStoreTables(OutputCollector dynamo) throws IOException;
}
