package io.fineo.batch.processing.spark;

import com.google.common.collect.Multimap;
import io.fineo.batch.processing.dynamo.FailedIngestFile;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.spark.convert.RowRecordConverter;
import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.batch.processing.spark.write.DynamoWriter;
import io.fineo.batch.processing.spark.write.StagedFirehoseWriter;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Run the stream processing pipeline in 'batch mode' as a spark job.
 */
public class BatchProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(BatchProcessor.class);
  private final BatchOptions opts;
  private IngestManifest manifest;

  public BatchProcessor(BatchOptions opts) {
    this.opts = opts;
  }

  public void run(JavaSparkContext context)
    throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    System.out.println("---Running processor");
    // singleton instance
    IngestManifest manifest = opts.getManifest();
    Multimap<String, String> sources = manifest.files();
    System.out.println("--- Got potential sources");
    if (sources.size() == 0) {
      LOG.warn("No input sources found!");
      return;
    }
    System.out.println("----Running job");
    BatchRddLoader loader = runJob(context, sources);
    System.out.println("----clearing manifest");
    clearManifest(manifest, sources, loader.getFilesThatFailedToLoad());
  }

  private void clearManifest(IngestManifest manifest, Multimap<String, String> sources,
    List<FailedIngestFile>
      filesThatFailedToLoad) {
    for (Map.Entry<String, Collection<String>> files : sources.asMap().entrySet()) {
      manifest.remove(files.getKey(), files.getValue());
    }
    for(FailedIngestFile failure: filesThatFailedToLoad) {
      manifest.addFailure(failure);
      manifest.remove(failure.getOrg(), failure.getFile());
    }
    manifest.flush();
  }

  private Multimap<String, String> loadManifest() {
    this.manifest = opts.getManifest();
    System.out.println("---Got manifest");
    return manifest.files();
  }

  private BatchRddLoader runJob(JavaSparkContext context, Multimap<String, String> sources)
    throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    BatchRddLoader loader = new BatchRddLoader(context, sources);
    System.out.println("Got batch loader");
    loader.load();
    System.out.println("Loaded batch");

    // convert the records
    List<JavaRDD<GenericRecord>> records = parseJson(context, loader.getJsonFiles());
    System.out.println("Parsed json");
    records.addAll(parseCsv(context, loader.getCsvFiles()));
    System.out.println("Parsed csv");

    // write the records
    List<JavaFutureAction> actions = new ArrayList<>(records.size() * 2);
    for (JavaRDD<GenericRecord> rdd : records) {
      actions.add(rdd.foreachPartitionAsync(new DynamoWriter(opts)));
      // we write them separately to firehose because we don't have a way of just saving files
      // directly to S3 without them being sequence files
      actions.add(rdd.foreachPartitionAsync(new StagedFirehoseWriter(opts)));
    }
    System.out.println("Processing records");

    // wait for all the actions to complete
    for (JavaFutureAction action : actions) {
      action.get();
    }
    System.out.println("All records processed");
    return loader;
  }

  private List<JavaRDD<GenericRecord>> parseJson(JavaSparkContext context,
    Multimap<String, Path> files) {
    List<JavaRDD<GenericRecord>> records = new ArrayList<>(files.size());
    SQLContext sqlContext = new SQLContext(context);
    for (Map.Entry<String, Collection<Path>> orgToFile : files.asMap().entrySet()) {
      for (Path file : orgToFile.getValue()) {
        DataFrame df = sqlContext.read().json(file.toString());
        JavaRDD<Row> rows = df.javaRDD();
        JavaRDD<GenericRecord> rdd = rows.map(new RowRecordConverter(orgToFile.getKey(), opts));
        rdd.persist(StorageLevel.MEMORY_AND_DISK());
        records.add(rdd);
      }
    }

    return records;
  }

  private List<JavaRDD<GenericRecord>> parseCsv(JavaSparkContext context,
    Multimap<String, Path> files) {
    SQLContext sqlContext = new SQLContext(context);
    List<JavaRDD<GenericRecord>> records = new ArrayList<>(files.size());
    for (Map.Entry<String, Collection<Path>> orgToFile : files.asMap().entrySet()) {
      for (Path file : orgToFile.getValue()) {
        DataFrame df = sqlContext.read()
                                 .format("com.databricks.spark.csv")
                                 .option("inferSchema", "false")
                                 .option("header", "true")
                                 .load(file.toString());
        JavaRDD<Row> rows = df.javaRDD();
        JavaRDD<GenericRecord> rdd = rows.map(new RowRecordConverter(orgToFile.getKey(), opts));
        rdd.persist(StorageLevel.MEMORY_AND_DISK());
        records.add(rdd);
      }
    }
    return records;
  }

  public static void main(String[] args) throws Exception {
    System.out.println("--- Starting batch processing ---");
    // parse arguments and load options
    BatchOptions opts = new BatchOptions();
    System.out.println("--- Created options ---");
    opts.setProps(PropertiesLoaderUtil.load());

    System.out.println("--- Loaded properties ---");

    // setup spark
    SparkConf conf = new SparkConf().setAppName(BatchProcessor.class.getName());
    System.out.println("--- Created conf ---");
    final JavaSparkContext context = new JavaSparkContext(conf);
    System.out.println("--- Created context ---");
    // run the job
    BatchProcessor processor = new BatchProcessor(opts);
    System.out.println("--- Created processor--");
    try {
      processor.run(context);
    } catch (Error e) {
      System.out.println("Got error!");
      System.out.println(Arrays.toString(e.getStackTrace()));
      // need to print the trace since Spark is not handling it correctly in 1.6.2
      e.printStackTrace();
      System.exit(1000);
      throw e;
    }
    System.out.println("^^^^^ App completed ^^^^^^");
  }
}
