/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fineo.etl.options;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.schemarepo.Repository;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ETLOptionBuilder {

  public static final String HELP_ARG = "h";
  private static final String LOCATION_ARG = "r";
  private static final String DIR_ARG = "s";
  private static final String SCHEMA_PROVIDER_ARG = "p";
  private static final String OUTPUT_DIR_ARG = "o";
  private static final String SCHEMA_DIR_ARG = "c";
  private static final String ERROR_DIR_ARG = "e";
  private static final String ARCHIVE_DIR_ARG = "a";
  private Options opts;

  private static Map<String, Repository> shortNameRepository = new HashMap<>();

  // setup the short name for easy access to the schema provider
  static {

  }

  public static ETLOptions build(String[] args) {
    CommandLineParser parser = new GnuParser();
    ETLOptionBuilder builder = new ETLOptionBuilder();
    try {
      CommandLine line = parser.parse(builder.getOptions(), args);
      return builder.buildOptions(line);
    } catch (ParseException e) {
      return new ETLOptions(builder.opts).error(e);
    }
  }

  private ETLOptionBuilder() {
  }

  private Options getOptions() {
    if (this.opts != null) {
      this.opts = createOpts();
    }

    return opts;
  }

  private Options createOpts() {
    Options opts = new Options();
    opts.addOption(HELP_ARG, "help", false, "Show this help");
    addRequired(opts, new Option(LOCATION_ARG, "rootdir", true,
      "Root directory where the ingest files are stored, e.g hdfs://the/root/dir or "
      + "s3://bucket/dir/key"));
    opts.addOption(DIR_ARG, "staging-directory", true,
      "Staging directory for ingest files. All files under this directory will be "
      + "included under for processing. Repeatable argument");
    addRequired(opts, new Option(SCHEMA_PROVIDER_ARG, "schema-provider", true,
      "Class name or short name of the current schema provider "));

    // output to be uploaded
    addRequired(opts, new Option(OUTPUT_DIR_ARG, "output-directory", true,
      "Full path to the directory under the rootdir where we should output successful "
      + "files"));
    addRequired(opts,
      new Option(SCHEMA_DIR_ARG, "schema-change-output-directory", true,
        "Full path to the directory under the rootdir where we should output files "
        + "for rows that had a schema change"));
    addRequired(opts,
      new Option(ERROR_DIR_ARG, "error-output-directory", true,
        "Full path to the directory under the rootdir where we should output files "
        + "for rows that produced an error (e.g. no tenant id)"));

    // output to be archived
    addRequired(opts,
      new Option(ARCHIVE_DIR_ARG, "archive-uri", true,
        "Full uri to the location where we should archive the deduped rows from the run."));
    return opts;
  }

  private void addRequired(Options opts, Option opt) {
    opt.setRequired(true);
    opts.addOption(opt);
  }

  private ETLOptions buildOptions(CommandLine line) {
    ETLOptions opts = new ETLOptions(this.opts);
    if (line.hasOption(HELP_ARG)) {
      return opts.withHelp();
    }

    opts.root(line.getOptionValue(LOCATION_ARG));

    if (line.hasOption(DIR_ARG)) {
      String[] dirs = line.getOptionValues(DIR_ARG);
      if (dirs == null || dirs.length == 0) {
        opts.error(new ParseException("Must specify at least one staging directory"));
        return opts;
      }
      opts.directories(dirs);
    }

    String schemaRepo = line.getOptionValue(SCHEMA_PROVIDER_ARG);
    Repository repo = shortNameRepository.get(schemaRepo);
    opts.schema(repo);

    opts.archive(line.getOptionValue(ARCHIVE_DIR_ARG));
    return opts;
  }
}
