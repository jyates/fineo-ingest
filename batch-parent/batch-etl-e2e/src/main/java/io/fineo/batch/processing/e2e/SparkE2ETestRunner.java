package io.fineo.batch.processing.e2e;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import io.fineo.e2e.options.LocalSchemaStoreOptions;
import io.fineo.etl.spark.SparkETL;
import io.fineo.etl.spark.options.ETLOptions;
import io.fineo.etl.spark.options.OptionsHandler;
import io.fineo.schema.store.SchemaStore;

/**
 * End-2-End test runner for SparkETL. Used here so we can only build a single jar
 */
public class SparkE2ETestRunner {

  public static void main(String[] args) throws Exception {
    LocalSchemaStoreOptions storeArgs = new LocalSchemaStoreOptions();
    ETLOptions opts = new ETLOptions();
    JCommander jc = new JCommander(new Object[]{storeArgs, opts});
    try {
      jc.parse(args);
    }catch(ParameterException e){
      jc.usage();
      throw e;
    }

    SchemaStore store = storeArgs.getStore();
    SparkETL.run(opts, store);
  }
}
