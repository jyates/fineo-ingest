package io.fineo.batch.processing.spark;

import com.google.common.collect.Multimap;
import io.fineo.batch.processing.dynamo.FailedIngestFile;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.spark.convert.ReadResult;
import io.fineo.batch.processing.spark.convert.RecordConverter;
import io.fineo.batch.processing.spark.options.BatchOptions;
import io.fineo.batch.processing.spark.write.DynamoWriter;
import io.fineo.batch.processing.spark.write.StagedFirehoseWriter;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.etl.spark.fs.RddUtils.getRddByKey;

/**
 * Run the stream processing pipeline in 'batch mode' as a spark job.
 */
public class BatchProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(BatchProcessor.class);
  private final BatchOptions opts;

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
    for (FailedIngestFile failure : filesThatFailedToLoad) {
      manifest.addFailure(failure);
      manifest.remove(failure.getOrg(), failure.getFile());
    }
    manifest.flush();
  }

  private BatchRddLoader runJob(JavaSparkContext context, Multimap<String, String> sources)
    throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    BatchRddLoader loader = new BatchRddLoader(context, sources);
    System.out.println("Got batch loader");
    loader.load();
    System.out.println("Loaded batch");

    // convert the records
    SQLContext sqlContext = new SQLContext(context);
    List<JavaPairRDD<ReadResult, Iterable<GenericRecord>>> records = parse(loader.getJsonFiles(),
      path -> sqlContext.read().json(path.toString()));
    System.out.println("Parsed json");
    records.addAll(parse(loader.getCsvFiles(),
      path -> sqlContext.read()
                        .format("com.databricks.spark.csv")
                        .option("inferSchema", "false")
                        .option("header", "true")
                        .load(path.toString())));
    System.out.println("Parsed csv");

    // group the records by the results
    List<JavaRDD<GenericRecord>> successes = new ArrayList<>();
    List<JavaPairRDD<ReadResult, Iterable<GenericRecord>>> failures = new ArrayList<>();
    for (JavaPairRDD<ReadResult, Iterable<GenericRecord>> rdd : records) {
      JavaRDD<GenericRecord> success =
        rdd.filter(v -> v._1().getOut() == ReadResult.Outcome.SUCCESS)
           .values()
           .flatMap(iter -> iter);
      successes.add(success);

      JavaPairRDD<ReadResult, Iterable<GenericRecord>> failure =
        rdd.filter(v -> v._1().getOut() == ReadResult.Outcome.FAILURE);
      failures.add(failure);
    }

    // group the failures
    JavaPairRDD<ReadResult, GenericRecord> failure = null;
    if (failures.size() > 0) {
      failure = failures.get(0).flatMapValues(v -> v);
      for (int i = 1; i < failures.size(); i++) {
        JavaPairRDD<ReadResult, Iterable<GenericRecord>> f2 = failures.get(i);
        failure.union(f2.flatMapValues(values -> values));
      }
    }

    writeSuccesses(context, successes.toArray(new JavaRDD[0]));

    if (failure != null) {
      System.out.println("Some failures found - writing them out");
      JavaPairRDD<String, Iterable<GenericRecord>> out =
        failure.mapToPair(tuple -> new Tuple2<>(tuple._1().getOrg(), newArrayList(tuple._2())))
               .flatMapValues(error -> error)
               .groupByKey();
      List<String> orgs = out.keys().collect();
      // write each org to its own directory
      String errors = opts.getErrorDirectory();
      for (String org : orgs) {
        JavaRDD<GenericRecord> orgFailedRecords = getRddByKey(out, org);
        orgFailedRecords.saveAsObjectFile(new Path(errors, org).toString());
      }
      System.out.println("Finished writing failures");
    }

    return loader;
  }

  private void writeSuccesses(JavaSparkContext context, JavaRDD[] javaRDDs)
    throws ExecutionException, InterruptedException {
    if (javaRDDs == null || javaRDDs.length == 0) {
      return;
    }
    List<JavaFutureAction> actions = new ArrayList<>(2);
    JavaRDD<GenericRecord> toWrite = context.union(javaRDDs);
    actions.add(toWrite.foreachPartitionAsync(new DynamoWriter(opts)));
    // we write them separately to firehose because we don't have a way of just saving files
    // directly to S3 without them being sequence files
    actions.add(toWrite.foreachPartitionAsync(new StagedFirehoseWriter(opts)));
    System.out.println("Processing records");

    // wait for all the actions to complete
    for (JavaFutureAction action : actions) {
      action.get();
    }
    System.out.println("All records processed");
  }

  private List<JavaPairRDD<ReadResult, Iterable<GenericRecord>>> parse(Multimap<String, Path> files,
    Function<Path, DataFrame> loader) {
    List<JavaPairRDD<ReadResult, Iterable<GenericRecord>>> records = new ArrayList<>(files.size());
    for (Map.Entry<String, Collection<Path>> orgToFile : files.asMap().entrySet()) {
      for (Path file : orgToFile.getValue()) {
        JavaRDD<Row> rows = loader.apply(file).javaRDD();
        JavaPairRDD<ReadResult, GenericRecord> rdd =
          rows.mapToPair(new RecordConverter(orgToFile.getKey(), opts));
        rdd.persist(StorageLevel.MEMORY_AND_DISK());
        records.add(rdd.groupByKey());
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
