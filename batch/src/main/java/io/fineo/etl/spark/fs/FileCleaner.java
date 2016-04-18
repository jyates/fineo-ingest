package io.fineo.etl.spark.fs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.Set;

/**
 * Cleaner for files of less than the specified size
 */
public class FileCleaner {
  public static final long ZERO_LENGTH_FILES = 0;
  public static final long PARQUET_MIN_SIZE = 517;
  private final FileSystem fs;

  public FileCleaner(FileSystem fs) {
    this.fs = fs;
  }

  public void clean(Set<String> dirs, long minFileSize) throws IOException {
    for (String dir : dirs) {
      Path root = new Path(dir);
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(root, true);
      while (files.hasNext()) {
        LocatedFileStatus file = files.next();
        if (file.isDirectory()) {
          continue;
        }
        if (file.getLen() <= minFileSize) {
          fs.delete(file.getPath(), false);
        }
      }
    }
  }
}
