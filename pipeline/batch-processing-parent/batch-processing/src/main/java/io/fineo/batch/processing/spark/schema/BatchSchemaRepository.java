package io.fineo.batch.processing.spark.schema;

import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.exception.SchemaExistsException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreCopier;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.Repository;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;

/**
 * Build a schema repository for the batch processing step. This basically does the following:
 * <ol>
 * <li>Find all the orgs involved in the execution</li>
 * <li>Get the latest schemas for each org</li>
 * <li>Provide those schemas as the latest/only across the job</li>
 * </ol>
 */
public class BatchSchemaRepository extends InMemoryRepository {

  public static BatchSchemaRepository createRepository(ValidatorFactory validators,
    IngestManifest manifest, Repository source) throws IOException, OldSchemaException {
    BatchSchemaRepository target = new BatchSchemaRepository(validators);

    SchemaStore store = new SchemaStore(source);
    SchemaStore targetStore = new SchemaStore(target);
    StoreCopier copier = new StoreCopier(store, targetStore);
    for(String org: manifest.files().keySet()){
      copier.copy(org);
    }

    return target;
  }

  private BatchSchemaRepository(ValidatorFactory validators) {
    super(validators);
  }
}
