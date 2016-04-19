package io.fineo.etl.spark;

import com.google.inject.Inject;

/**
 * Run the stream processing pipeline in 'batch mode' as a spark job.
 */
public class BatchProcessor {

  private final BatchOptions opts;

  @Inject
  public BatchProcessor(BatchOptions opts) {
    this.opts = opts;
  }


  public void run() {

  }
}
