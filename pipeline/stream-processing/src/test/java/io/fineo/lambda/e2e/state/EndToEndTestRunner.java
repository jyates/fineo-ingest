package io.fineo.lambda.e2e.state;

import com.google.common.base.Preconditions;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.e2e.validation.EndToEndValidator;
import io.fineo.lambda.util.IResourceManager;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.exception.SchemaTypeNotFoundException;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.SchemaTestUtils;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper utility to implement an end-to-end test of the lambda architecture
 */
public class EndToEndTestRunner {

  private static final Log LOG = LogFactory.getLog(EndToEndTestRunner.class);

  private final LambdaClientProperties props;
  private final IResourceManager manager;
  private final EndToEndValidator validator;
  private final EndtoEndSuccessStatus status;
  private EventFormTracker progress;

  public EndToEndTestRunner(LambdaClientProperties props, IResourceManager manager,
    EndToEndValidator validator) throws Exception {
    this.props = props;
    this.manager = manager;
    this.validator = validator;
    this.status = new EndtoEndSuccessStatus();
  }

  public static void updateSchemaStore(SchemaStore store, Map<String, Object> event)
    throws Exception {
    String orgId = (String) event.get(AvroSchemaProperties.ORG_ID_KEY);
    String metricType = (String) event.get(AvroSchemaProperties.ORG_METRIC_TYPE_KEY);
    Preconditions.checkArgument(orgId != null && metricType != null);
    // collect the fields that are not the base fields
    List<String> otherFields = event.keySet().stream().filter(AvroSchemaProperties
      .IS_BASE_FIELD.negate()).collect(Collectors.toList());
    try {
      SchemaTestUtils.addNewOrg(store, orgId, metricType, otherFields.toArray(new String[0]));
    } catch (IllegalStateException e) {
      StoreManager manager = new StoreManager(store);
      StoreClerk clerk = new StoreClerk(store, orgId);
      StoreManager.OrganizationBuilder builder = manager.updateOrg(orgId);
      StoreManager.MetricBuilder metricBuilder = builder.updateMetric(metricType);
      event.entrySet().stream().sequential()
           .filter(entry -> AvroSchemaProperties.IS_BASE_FIELD.negate().test(entry.getKey()))
           .filter(entry -> {
             try {
               return clerk.getMetricForUserNameOrAlias(metricType)
                           .getCanonicalNameFromUserFieldName(entry.getKey()) == null;
             } catch (SchemaNotFoundException e1) {
               throw new RuntimeException(e1);
             }
           })
           .forEach(entry -> {
             try {
               String clazz = entry.getValue().getClass().getSimpleName().toUpperCase();
               if (clazz.equals("BYTE[]")) {
                 metricBuilder.newField().withType("BYTES").withName(entry.getKey()).build();
                 return;
               } else if (clazz.equals("INTEGER")) {
                 metricBuilder.newField().withType("INTEGER").withName(entry.getKey()).build();
                 return;
               }
               Schema.Type type = Schema.Type.valueOf(clazz);
               switch (type) {
                 case BOOLEAN:
                 case LONG:
                 case FLOAT:
                 case DOUBLE:
                 case STRING:
                   metricBuilder.newField().withType(type.getName()).withName(entry.getKey())
                                .build();
                   return;
               }
             } catch (SchemaTypeNotFoundException e1) {
               e1.printStackTrace();
             }
           });

      metricBuilder.build();
      builder.commit();
    }
  }


  private static <T> List<T> collectMapListValues(Map<?, List<T>> map) {
    return map.values().stream()
              .sequential()
              .flatMap(list -> list.stream())
              .collect(Collectors.toList());
  }


  public void setup() throws Exception {
    this.manager.setup();
    this.progress = new EventFormTracker();
  }

  public void run(Map<String, Object> json) throws Exception {
    register(json);
    send(json);
  }

  public void register(Map<String, Object> json) throws Exception {
    updateSchemaStore(manager.getStore(), json);
    this.status.updated();
  }

  public void send(Map<String, Object> json) throws Exception {
    progress.sending(json);
    this.progress.sent(this.manager.send(json));
    this.status.sent();
  }


  public void validate() throws Exception {
    validator.validate(manager, props, progress);
    status.success();
  }

  public void cleanup() throws Exception {
    this.manager.cleanup(status);
  }

  public LambdaClientProperties getProps() {
    return this.props;
  }

  public EventFormTracker getProgress() {
    return this.progress;
  }

  public IResourceManager getManager() {
    return manager;
  }
}
