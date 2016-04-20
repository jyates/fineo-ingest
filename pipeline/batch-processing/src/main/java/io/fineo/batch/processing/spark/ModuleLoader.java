package io.fineo.batch.processing.spark;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Guice;
import com.google.inject.Module;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.SchemaStoreModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.configure.firehose.FirehoseFunctions;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.lambda.handle.staged.FirehosePropertyBridge;
import io.fineo.lambda.handle.staged.RecordToDynamoHandler;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Helper class to get the modules necessary for loading a given class. Mostly used to facilitate
 * local testing
 */
public class ModuleLoader implements Serializable {

  private Multimap<Class<?>, Module> classes = ArrayListMultimap.create();

  public Collection<Module> getModules(Class<?> clazz) {
    return classes.get(clazz);
  }

  public <T> T load(Class<T> clazz) {
    return Guice.createInjector(getModules(clazz)).getInstance(clazz);
  }

  public void set(Class<?> clazz, List<Module> modules) {
    classes.putAll(clazz, modules);
  }

  /**
   * Creates the default module loader for the expected classes
   */
  public static ModuleLoader create(Properties properties) {
    ModuleLoader loader = new ModuleLoader();

    // common modules
    PropertiesModule props = new PropertiesModule(properties);
    DefaultCredentialsModule credentials = new DefaultCredentialsModule();
    DynamoModule dynamo = new DynamoModule();
    DynamoRegionConfigurator dynamoConfig = new DynamoRegionConfigurator();
    SchemaStoreModule store = new SchemaStoreModule();

    loader.set(RawJsonToRecordHandler.class, newArrayList(
      props,
      credentials,
      store,
      dynamo, dynamoConfig
    ));

    loader.set(RecordToDynamoHandler.class, newArrayList(
      props,
      credentials,
      dynamo, dynamoConfig
    ));

    loader.set(FirehoseBatchWriter.class, newArrayList(
      props,
      credentials,
      new FirehoseModule(), new FirehoseFunctions(), new FirehosePropertyBridge()
    ));
    return loader;
  }
}
