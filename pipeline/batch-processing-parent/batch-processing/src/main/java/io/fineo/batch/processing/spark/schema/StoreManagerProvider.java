package io.fineo.batch.processing.spark.schema;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import org.schemarepo.Repository;

/**
 *
 */
public class StoreManagerProvider extends AbstractModule {

  @Override
  protected void configure() {
  }

  @Inject
  @Provides
  @Singleton
  public SchemaStore getStore(Repository repository) {
    return new SchemaStore(repository);
  }

  @Inject
  @Provides
  @Singleton
  public StoreManager getStoreManager(SchemaStore store) {
    return new StoreManager(store);
  }
}
