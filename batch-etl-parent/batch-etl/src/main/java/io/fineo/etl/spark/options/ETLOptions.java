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
package io.fineo.etl.spark.options;

import com.beust.jcommander.Parameter;
import io.fineo.etl.spark.SparkETL;

/**
 * Bean for storing the current options and helper methods for things like printing the help.
 */
public class ETLOptions {

  @Parameter(names = "--help", help = true)
  private boolean help;
  @Parameter(names = "--source", description = "Directory where the input files are stored")
  private String root;
  @Parameter(names = "--completed", description = "Base directory to write the processed files")
  private String completed;
  @Parameter(names = "--archive", description = "Directory in which to archiveDir the input files")
  private String archive;

  public boolean help() {
    return this.help;
  }

  public ETLOptions withHelp() {
    this.help = true;
    return this;
  }

  public void source(String root) {
    this.root = root;
  }

  public String source() {
    return this.root;
  }

  public String completed() {
    return this.completed + "/" + SparkETL.VERSION;
  }

  public void completed(String archiveUri) {
    this.completed = archiveUri;
  }

  public String archiveDir() {
    return this.archive;
  }

  public void archiveDir(String completedDir) {
    this.archive = completedDir;
  }
}
