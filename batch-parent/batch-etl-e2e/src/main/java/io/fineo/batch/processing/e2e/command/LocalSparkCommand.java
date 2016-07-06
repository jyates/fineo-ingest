package io.fineo.batch.processing.e2e.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.fineo.etl.spark.SparkETL;
import io.fineo.etl.spark.options.ETLOptions;
import io.fineo.schema.store.SchemaStore;
import io.fineo.spark.rule.LocalSpark;

import java.io.File;

@Parameters(commandNames = "local",
            commandDescription = "Run the Spark processing from a local Spark cluster")
public class LocalSparkCommand extends SparkCommand {

  @Parameter(names = "--input", description = "Avro encoded file to process")
  public String input;

  @Parameter(names = "--output", description = "Directory where the processed files should be"
                                                   + " written")
  public String outputDir;

  @Parameter(names = "--archive", description = "Directory where files should be archived")
  public String archive;

  @Override
  public void run(SchemaStore store) throws Exception {
    LocalSpark spark = new LocalSpark();
    spark.setup();

    SparkETL etl = new SparkETL(getOpts());
    etl.run(spark.jsc(), store);

    spark.stop();
  }

  private ETLOptions getOpts() {
    ETLOptions opts = new ETLOptions();
    String base = "file://";
    opts.source(base + new File(input).getAbsolutePath());

    File output = new File(outputDir);
    opts.completed(base + output.getAbsolutePath());

    File archiveDir = new File(archive);
    opts.archive(base + archiveDir.getAbsolutePath());
    return opts;
  }
}
