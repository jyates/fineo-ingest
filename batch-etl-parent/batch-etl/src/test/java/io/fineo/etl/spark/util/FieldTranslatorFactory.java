package io.fineo.etl.spark.util;

import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;

/**
 * Translate a raw json event into its canonical names
 */
public class FieldTranslatorFactory {

  private final SchemaStore store;

  public FieldTranslatorFactory(SchemaStore store) {
    this.store = store;
  }

  public FieldTranslator translate(String orgId, String aliasMetricName)
    throws SchemaNotFoundException {
    StoreClerk manager = new StoreClerk(store, orgId);
    StoreClerk.Metric metric = manager.getMetricForUserNameOrAlias(aliasMetricName);
    return new FieldTranslator(metric);
  }

  public class FieldTranslator {
    private final StoreClerk.Metric metric;

    public FieldTranslator(StoreClerk.Metric metric) {
      this.metric = metric;
    }

    public String translate(String alias) {
      return metric.getCanonicalNameFromUserFieldName(alias);
    }

    public Pair<String, Object> translate(String alias, Map<String, Object> customerEvent) {
      String fieldName = translate(alias);

      // it an unknown field
      if(fieldName == null){
        fieldName = alias;
      }

      // map the metric field to its canonical name
      if (fieldName.equals(AvroSchemaProperties.ORG_METRIC_TYPE_KEY)) {
        return new ImmutablePair<>(fieldName, metric.getMetricId());
      }
      // otherwise, we just use the canonical name -> event raw value
      return new ImmutablePair<>(fieldName, customerEvent.get(alias));
    }
  }
}
