package io.fineo.etl.spark.util;

import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;

/**
 * Translate a raw json event into its canonical names
 */
public class FieldTranslatorFactory {

  private final SchemaStore store;

  public FieldTranslatorFactory(LambdaClientProperties props) {
    this(props.createSchemaStore());
  }

  public FieldTranslatorFactory(SchemaStore store) {
    this.store = store;
  }

  public FieldTranslator translate(String orgId, String aliasMetricName) {
    AvroSchemaManager manager = new AvroSchemaManager(store, orgId);

    Metric metric = manager.getMetricInfo(aliasMetricName);
    Map<String, String> aliasToName = AvroSchemaManager.getAliasRemap(metric);
    return new FieldTranslator(manager, aliasToName);
  }

  public class FieldTranslator {
    private final Map<String, String> map;
    private final AvroSchemaManager manager;

    public FieldTranslator(AvroSchemaManager manager, Map<String, String> aliasToFieldMap) {
      this.manager = manager;
      this.map = aliasToFieldMap;
    }

    public String translate(String alias) {
      if (AvroSchemaEncoder.IS_BASE_FIELD.test(alias)) {
        return alias;
      }
      return map.get(alias);
    }

    public Pair<String, Object> translate(String alias, Map<String, Object> customerEvent) {
      String fieldName = translate(alias);

      // it an unknown field
      if(fieldName == null){
        fieldName = alias;
      }
      // map the metric field to its canonical name
      if (fieldName.equals(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY)) {
        Map<String, String> fieldMap =
          AvroSchemaManager.getAliasRemap(manager.getOrgMetadata());
        String canonicalName = fieldMap.get(customerEvent.get(fieldName));
        return new ImmutablePair<>(fieldName, canonicalName);
      }
      // otherwise, we just use the canonical name -> event raw value
      return new ImmutablePair<>(fieldName, customerEvent.get(alias));
    }
  }
}
