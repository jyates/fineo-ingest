package io.fineo.batch.processing.spark;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.spark.options.BatchOptions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.List;

/**
 * A set of batch options that uses a local, mock {@link IngestManifest}
 */
public class LocalMockBatchOptions extends BatchOptions {
  private List<Pair<String, String>> ingestPath;

  public void setInput(String ingestPath) {
    setInput(new ImmutablePair<>("local", ingestPath));
  }

  public void setInput(Pair<String, String>... ingestPaths) {
    this.ingestPath = Arrays.asList(ingestPaths);
  }

  @Override
  public IngestManifest getManifest() {
    return new FakeManifest(this.ingestPath);
  }

  private class FakeManifest extends IngestManifest {

    private final Multimap<String, String> files;

    public FakeManifest(List<Pair<String, String>> paths) {
      super(null, null, null);
      this.files = ArrayListMultimap.create();
      for (Pair<String, String> orgFile: paths){
        files.put(orgFile.getKey(), orgFile.getValue());
      }
    }

    @Override
    public Multimap<String, String> files() {
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
