package io.fineo.lambda.e2e.aws.dynamo;

import com.google.inject.Injector;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.manager.IDynamoResource;
import io.fineo.lambda.e2e.manager.ManagerBuilder;
import io.fineo.lambda.e2e.manager.collector.OutputCollector;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.configure.util.InstanceToNamed.property;
import static io.fineo.lambda.e2e.validation.util.ValidationUtils.verifyRecordMatchesJson;
import static io.fineo.lambda.e2e.validation.util.ValidationUtils.verifyRecordsMatchJson;
import static org.junit.Assert.assertEquals;

/**
 * Dynamo resource that wraps an actual dynamo table
 */
public class DelegateAwsDynamoResource implements IDynamoResource {

  private DynamoResource dynamo;
  private Injector injector;

  @Override
  public void init(Injector injector) {
    this.injector = injector;
    this.dynamo = injector.getInstance(DynamoResource.class);
    FutureWaiter waiter = injector.getInstance(FutureWaiter.class);
    dynamo.setup(waiter);
  }

  @Override
  public void cleanup(FutureWaiter waiter) {
    this.dynamo.cleanup(waiter);
  }

  @Override
  public AvroToDynamoWriter getWriter() {
    return injector.getInstance(AvroToDynamoWriter.class);
  }

  @Override
  public void verify(Stream<RecordMetadata> metadata, List<Map<String, Object>> json) {
    List<GenericRecord> records =
      metadata.map(meta -> dynamo.read(meta)).flatMap(r -> r.stream()).collect(Collectors.toList());
    verifyRecordsMatchJson(getStore(), records, json);
  }

  @Override
  public void copyStoreTables(OutputCollector collector) {
    this.dynamo.copyStoreTables(collector);
  }

  private SchemaStore getStore() {
    return injector.getInstance(SchemaStore.class);
  }

  public static void addAwsDynamo(ManagerBuilder builder) {
    builder.withDynamo(new DelegateAwsDynamoResource(),
      new DynamoModule(),
      new DynamoRegionConfigurator(),
      property(FineoProperties.DYNAMO_REGION, builder.getRegion()));
  }
}
