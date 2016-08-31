package io.fineo.etl.spark.options;

import com.beust.jcommander.JCommander;

public class OptionsHandler {

  public static ETLOptions handle(String[] args) {
    ETLOptions opts = new ETLOptions();
    JCommander cmd = new JCommander(opts);
    cmd.parse(args);

    if (opts.help()) {
      cmd.usage();
      System.exit(0);
    }
    return opts;
  }
}
