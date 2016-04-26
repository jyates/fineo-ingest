package io.fineo.batch.processing.spark;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.spark.options.BatchOptions;

/**
 * A set of batch options that uses a local, mock {@link IngestManifest}
 */
public class LocalMockBatchOptions extends BatchOptions {
  private String ingestPath;

  public void setInput(String ingestPath) {
    this.ingestPath = ingestPath;
  }

  @Override
  public IngestManifest getManifest() {
    return new FakeManifest(this.ingestPath);
  }

  private class FakeManifest extends IngestManifest {
    private final String path;

    public FakeManifest(String path) {
      super(null, null, null, null);
      this.path = path;
    }

    @Override
    public Multimap<String, String> files() {
      Multimap<String, String> files = ArrayListMultimap.create();
      files.put("local", this.path);
      return files;
    }

    @Override
    public void remove(String org, Iterable<String> files) {
      // noop
    }

    @Override
    public void add(String orgId, String s3location) {
      //noop
    }

    @Override
    public void load() {
      //noop
    }

    @Override
    public void flush() {
      //
    }
  }
}
