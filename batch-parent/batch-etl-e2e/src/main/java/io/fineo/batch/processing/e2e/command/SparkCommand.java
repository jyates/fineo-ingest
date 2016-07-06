package io.fineo.batch.processing.e2e.command;

import io.fineo.schema.store.SchemaStore;

/**
 *
 */
public abstract class SparkCommand {

  public abstract void run(SchemaStore store) throws Exception;
}
