package io.fineo.batch.processing.spark;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.batch.processing.dynamo.FailedIngestFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Load an RDD from files under a specified directory
 */
public class BatchRddLoader {
  private static final Logger LOG = LoggerFactory.getLogger(BatchRddLoader.class);

  public static final String S3_TYPE_PREFIX = "s3://";
  private final JavaSparkContext context;
  private final Multimap<String, String> orgToFiles;
  private FileSystem fs;
  private Multimap<String, Path> jsonFiles = ArrayListMultimap.create();
  private Multimap<String, Path> csvFiles = ArrayListMultimap.create();
  private List<Path> sources = new ArrayList<>();
  private List<FailedIngestFile> failedToLoad = new ArrayList<>();

  public BatchRddLoader(JavaSparkContext context, Multimap<String, String> orgToFiles) {
    this.context = context;
    this.orgToFiles = orgToFiles;
  }

  public void load() throws URISyntaxException, IOException {
    for (Map.Entry<String, Collection<String>> orgFileEntry : orgToFiles.asMap().entrySet()) {
      for (String file : orgFileEntry.getValue()) {
        URI root = new URI(file);
        if (!root.isAbsolute()) {
          root = new URI(S3_TYPE_PREFIX + file);
        }

        String org = orgFileEntry.getKey();
        this.fs = FileSystem.get(root, context.hadoopConfiguration());

        // find all the files under the given root directory
        RemoteIterator<LocatedFileStatus> iter = null;
        try {
          Path rootPath = fs.resolvePath(new Path(root.getPath()));
          iter = fs.listFiles(rootPath, true);
        } catch (AmazonS3Exception | FileNotFoundException e) {
          failedToLoad
            .add(new FailedIngestFile(org, file, e.getMessage()));
          LOG.warn("Failed to load file: {} for org '{}'", root, org, e);
          continue;
        }
        while (iter.hasNext()) {
          LocatedFileStatus status = iter.next();
          if (!status.isDirectory()) {
            sources.add(status.getPath());
            String path = status.getPath().toString();
            if (path.endsWith(".csv") || path.endsWith(".csv.gz")) {
              csvFiles.put(org, status.getPath());
            } else {
              jsonFiles.put(org, status.getPath());
            }
          }
        }
      }
    }
  }

  public Multimap<String, Path> getJsonFiles() {
    return this.jsonFiles;
  }

  public Multimap<String, Path> getCsvFiles() {
    return this.csvFiles;
  }

  public List<FailedIngestFile> getFilesThatFailedToLoad() {
    return failedToLoad;
  }
}
