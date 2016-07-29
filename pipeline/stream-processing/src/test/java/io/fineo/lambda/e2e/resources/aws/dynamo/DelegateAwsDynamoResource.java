package io.fineo.lambda.e2e.resources.aws.dynamo;

import com.google.inject.Injector;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.configure.util.InstanceToNamed;
import io.fineo.lambda.dynamo.DynamoTestConfiguratorModule;
import io.fineo.lambda.dynamo.avro.AvroToDynamoWriter;
import io.fineo.lambda.e2e.resources.manager.collector.OutputCollector;
import io.fineo.lambda.e2e.resources.manager.IDynamoResource;
import io.fineo.lambda.e2e.resources.manager.ManagerBuilder;
import io.fineo.lambda.util.run.FutureWaiter;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.e2e.validation.util.ValidationUtils.verifyRecordMatchesJson;
import static org.junit.Assert.assertEquals;

/**
 * Dynamo resource that wraps an actual dynamo table
 */
public class DelegateAwsDynamoResource implements IDynamoResource {

  private DynamoResource dynamo;
  private Injector injector;

  @Override
  public void init(Injector injector) {
    this.dynamo = injector.getInstance(DynamoResource.class);
    this.injector = injector;
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
  public void verify(RecordMetadata metadata, Map<String, Object> json) {
    List<GenericRecord> records = dynamo.read(metadata);
    assertEquals(newArrayList(records.get(0)), records);
    verifyRecordMatchesJson(getStore(), json, records.get(0));
  }

  @Override
  public void copyStoreTables(OutputCollector collector) {
    this.dynamo.copyStoreTables(collector);
  }

  private SchemaStore getStore() {
    return injector.getInstance(SchemaStore.class);
  }

  public static void addLocalDynamo(ManagerBuilder builder, String url) {
    builder.withDynamo(new DelegateAwsDynamoResource(),
      new DynamoModule(),
      new DynamoTestConfiguratorModule(),
      InstanceToNamed.property(FineoProperties.DYNAMO_URL_FOR_TESTING, url));
  }

  public static void addAwsDynamo(ManagerBuilder builder){
    builder.withDynamo(new DelegateAwsDynamoResource(),
      new DynamoModule(),
      new DynamoRegionConfigurator(),
      InstanceToNamed.property(FineoProperties.DYNAMO_REGION, builder.getRegion()));
  }
}
