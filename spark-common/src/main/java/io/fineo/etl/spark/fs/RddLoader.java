package io.fineo.etl.spark.fs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Load an RDD from files under a specified directory
 */
public class RddLoader {

  private final JavaSparkContext context;
  private final Collection<String> rootStrings;
  private FileSystem fs;
  private Path[] jsonRdds;
  private List<Path> sources;
  private Path[] csvFiles;

  public RddLoader(JavaSparkContext context, Collection<String> root) {
    this.context = context;
    this.rootStrings = root;
  }

  public void load() throws URISyntaxException, IOException {
    for(String rootString: rootStrings) {
      URI root = new URI(rootString);
      this.fs = FileSystem.get(root, context.hadoopConfiguration());

      // find all the files under the given root directory
      Path rootPath = fs.resolvePath(new Path(root.getPath()));
      List<Path> sources = new ArrayList<>();
      List<Path> jsonSource = new ArrayList<>();
      List<Path> csvSource = new ArrayList<>();
      RemoteIterator<LocatedFileStatus> iter = fs.listFiles(rootPath, true);
      while (iter.hasNext()) {
        LocatedFileStatus status = iter.next();
        if (!status.isDirectory()) {
          sources.add(status.getPath());
          String path = status.getPath().toString();
          if(path.endsWith(".csv") || path.endsWith(".csv.gz")){
            csvSource.add(status.getPath());
          }else {
            jsonSource.add(status.getPath());
          }
        }
      }

      // get each file in the staging area
      this.jsonRdds = jsonSource.toArray(new Path[0]);
      this.csvFiles = csvSource.toArray(new Path[0]);
      this.sources = sources;
    }
  }

  public Path[] getJsonRdds() {
    return this.jsonRdds;
  }

  public Path[] getCsvFiles(){
    return this.csvFiles;
  }

  public void archive(String completed) throws IOException {
    Path archive = new Path(completed, Long.toString(System.currentTimeMillis()));
    boolean success = fs.mkdirs(archive);
    if (!success) {
      if (!fs.exists(archive)) {
        throw new IOException("Could not create completed archive directory:" + archive);
      }
    }
    for (Path source : sources) {
      Path target = new Path(archive, source.getName());
      if (!fs.rename(source, target)) {
        throw new IOException("Could not archive " + source + " -> " + target);
      }
    }
  }

  public FileSystem getFs() {
    return this.fs;
  }
}
