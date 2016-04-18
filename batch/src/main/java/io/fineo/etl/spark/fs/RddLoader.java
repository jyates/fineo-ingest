package io.fineo.etl.spark.fs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class RddLoader {

  private final JavaSparkContext context;
  private final String rootString;
  private URI root;
  private FileSystem fs;
  private JavaPairRDD<String, PortableDataStream>[] rdds;
  private List<Path> sources;

  public RddLoader(JavaSparkContext context, String root) {
    this.context = context;
    this.rootString = root;
  }

  public void load() throws URISyntaxException, IOException {
    this.root = new URI(rootString);
    this.fs = FileSystem.get(root, context.hadoopConfiguration());

    // find all the files under the given root directory
    Path rootPath = fs.resolvePath(new Path(root.getPath()));
    List<Path> sources = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(rootPath, true);
    while (iter.hasNext()) {
      LocatedFileStatus status = iter.next();
      if (!status.isDirectory()) {
        sources.add(status.getPath());
      }
    }

    // get each file in the staging area
    JavaPairRDD<String, PortableDataStream>[] stringRdds = new JavaPairRDD[sources.size()];
    for (int i = 0; i < sources.size(); i++) {
      stringRdds[i] = context.binaryFiles(sources.get(i).toString());
    }
    this.rdds = stringRdds;
    this.sources = sources;
  }

  public JavaPairRDD<String, PortableDataStream>[] getRdds() {
    return rdds;
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
