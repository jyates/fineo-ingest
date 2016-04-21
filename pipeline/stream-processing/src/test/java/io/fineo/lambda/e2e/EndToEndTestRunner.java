package io.fineo.lambda.e2e;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.lambda.util.ResourceManager;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
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
  private final ResourceManager manager;
  private final EndToEndValidator validator;
  private final EndtoEndSuccessStatus status;
  private ProgressTracker progress;

  public EndToEndTestRunner(LambdaClientProperties props, ResourceManager manager, EndToEndValidator validator)
    throws Exception {
    this.props = props;
    this.manager = manager;
    this.validator = validator;
    this.status = new EndtoEndSuccessStatus();
  }

  public static void updateSchemaStore(SchemaStore store, Map<String, Object> event)
    throws Exception {
    String orgId = (String) event.get(AvroSchemaEncoder.ORG_ID_KEY);
    String metricType = (String) event.get(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY);
    Preconditions.checkArgument(orgId != null && metricType != null);
    // collect the fields that are not the base fields
    List<String> otherFields = event.keySet().stream().filter(AvroSchemaEncoder
      .IS_BASE_FIELD.negate()).collect(Collectors.toList());
    try {
      SchemaTestUtils.addNewOrg(store, orgId, metricType, otherFields.toArray(new String[0]));
    } catch (IllegalStateException e) {
      // need to update the schema for the org
      Metadata metadata = store.getOrgMetadata(orgId);
      Metric metric = store.getMetricMetadataFromAlias(metadata, metricType);
      SchemaBuilder builder = SchemaBuilder.create();
      SchemaBuilder.MetricBuilder metricBuilder = builder.updateOrg(metadata).updateSchema(metric);
      event.entrySet().stream().sequential()
           .filter(entry -> AvroSchemaEncoder.IS_BASE_FIELD.negate().test(entry.getKey()))
           .filter(entry -> !collectMapListValues(metric.getMetadata().getCanonicalNamesToAliases())
             .contains(entry.getKey()))
           .forEach(entry -> {
             String clazz = entry.getValue().getClass().getSimpleName().toUpperCase();
             if (clazz.equals("BYTE[]")) {
               metricBuilder.withBytes(entry.getKey()).asField();
               return;
             } else if (clazz.equals("INTEGER")) {
               metricBuilder.withInt(entry.getKey()).asField();
               return;
             }
             Schema.Type type = Schema.Type.valueOf(clazz);
             switch (type) {
               case BOOLEAN:
                 metricBuilder.withBoolean(entry.getKey()).asField();
                 return;
               case LONG:
                 metricBuilder.withLong(entry.getKey()).asField();
                 return;
               case FLOAT:
                 metricBuilder.withFloat(entry.getKey()).asField();
                 return;
               case DOUBLE:
                 metricBuilder.withDouble(entry.getKey()).asField();
                 return;
               case STRING:
                 metricBuilder.withString(entry.getKey()).asField();
                 return;
             }
           });

      store.updateOrgMetric(metricBuilder.build().build(), metric);
    }
  }


  private static <T> List<T> collectMapListValues(Map<?, List<T>> map) {
    return map.values().stream()
              .sequential()
              .flatMap(list -> list.stream())
              .collect(Collectors.toList());
  }


  public void setup() throws Exception {
    this.manager.setup(props);
    this.progress = new ProgressTracker();
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

  public ProgressTracker getProgress() {
    return this.progress;
  }
}
