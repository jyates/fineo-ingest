package io.fineo.etl;

import io.fineo.internal.customer.Metric;
import io.fineo.lambda.configure.LambdaClientProperties;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;

import java.util.Map;

/**
 *
 */
public class FieldTranslatorFactory {

  private final SchemaStore store;

  public FieldTranslatorFactory(LambdaClientProperties props){
    this(props.createSchemaStore());
  }

  public FieldTranslatorFactory(SchemaStore store) {
    this.store = store;
  }

  public FieldTranslator translate(String orgId, String aliasMetricName){
    AvroSchemaManager manager = new AvroSchemaManager(store, orgId);
    Metric metric = manager.getMetricInfo(aliasMetricName);
    Map < String, String > aliasToName = AvroSchemaManager.getAliasRemap(metric);
    return new FieldTranslator(aliasToName);
  }

  public class FieldTranslator {
    private final Map<String, String> map;

    public FieldTranslator(Map<String, String> aliasToFieldMap) {
      this.map = aliasToFieldMap;
    }

    public String translate(String alias) {
      if (AvroSchemaEncoder.IS_BASE_FIELD.test(alias)) {
        return alias;
      }
      return map.get(alias);
    }
  }
}
