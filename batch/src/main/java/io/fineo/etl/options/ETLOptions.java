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

import io.fineo.etl.SparkETL;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.schemarepo.Repository;

/**
 * Bean for storing the current options and helper methods for things like printing the help.
 */
public class ETLOptions {
  private boolean help;
  private Options opts;
  private String error;
  private String[] directories;
  private String root;
  private String archive;
  private Repository repo;

  public ETLOptions(Options opts) {
    this.opts = opts;
  }

  public boolean help() {
    return this.help;
  }

  public void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("$SPARK_HOME/bin/spark-submit --class " + SparkETL.class.getName() + " "
                        + "[jar] [args]", opts);
  }

  public ETLOptions withHelp() {
    this.help = true;
    return this;
  }

  public ETLOptions error(ParseException e) {
    this.error = e.getMessage();
    return this;
  }

  public boolean error() {
    return this.error != null;
  }

  public void directories(String[] directories) {
    this.directories = directories;
  }

  public void root(String root) {
    this.root = root;
  }

  public String[] directories() {
    return this.directories;
  }

  public String root() {
    return this.root;
  }

  public String archive() {
    return this.archive;
  }

  public void archive(String archiveUri) {
    this.archive = archiveUri;
  }

  public void schema(Repository repo) {
    this.repo = repo;
  }
}
