package io.fineo.batch.processing.spark;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Load an RDD from files under a specified directory
 */
public class BatchRddLoader {
  public static final String S3_TYPE_PREFIX = "s3://";
  private final JavaSparkContext context;
  private final Multimap<String, String> orgToFiles;
  private FileSystem fs;
  private Multimap<String, Path> jsonFiles = ArrayListMultimap.create();
  private Multimap<String, Path> csvFiles = ArrayListMultimap.create();
  private List<Path> sources = new ArrayList<>();

  public BatchRddLoader(JavaSparkContext context, Multimap<String, String> orgToFiles) {
    this.context = context;
    this.orgToFiles = orgToFiles;
  }

  public void load() throws URISyntaxException, IOException {
    for (Map.Entry<String, Collection<String>> orgFileEntry : orgToFiles.asMap().entrySet()) {
      for (String file : orgFileEntry.getValue()) {
        URI root = new URI(file);
        if(!root.isAbsolute()){
          root = new URI(S3_TYPE_PREFIX + file);
        }

        this.fs = FileSystem.get(root, context.hadoopConfiguration());

        // find all the files under the given root directory
        System.out.println("Working directory:" + fs.getWorkingDirectory());
        Path rootPath = fs.resolvePath(new Path(root.getPath()));
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(rootPath, true);
        while (iter.hasNext()) {
          LocatedFileStatus status = iter.next();
          if (!status.isDirectory()) {
            sources.add(status.getPath());
            String path = status.getPath().toString();
            if (path.endsWith(".csv") || path.endsWith(".csv.gz")) {
              csvFiles.put(orgFileEntry.getKey(), status.getPath());
            } else {
              jsonFiles.put(orgFileEntry.getKey(), status.getPath());
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
}
