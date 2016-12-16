package io.fineo.lambda.e2e.manager;

import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.manager.collector.OutputCollector;
import io.fineo.schema.avro.RecordMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public interface IDynamoResource extends IResource {

  AvroToDynamoWriter getWriter();

  void verify(Stream<RecordMetadata> metadata, List<Map<String, Object>> json);

  void copyStoreTables(OutputCollector dynamo) throws IOException;
}
