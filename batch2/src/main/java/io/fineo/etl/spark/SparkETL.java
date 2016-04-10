package io.fineo.etl.spark;/*
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

import io.fineo.etl.SeekableDataInput;
import io.fineo.etl.options.ETLOptionBuilder;
import io.fineo.etl.options.ETLOptions;
import org.apache.avro.file.MultiSchemaFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class SparkETL {

  public static void main(String[] args) throws URISyntaxException {
    // parse arguments
    ETLOptions opts = ETLOptionBuilder.build(args);
    if (opts.help() || opts.error()) {
      opts.printHelp();
      System.exit(opts.error() ? 1 : 0);
    }

    SparkConf conf = new SparkConf().setAppName(SparkETL.class.getName());
    final JavaSparkContext context = new JavaSparkContext(conf);
    // setup RDD for each staging data source
    URI root = new URI(opts.root());
    List<URI> sources = new ArrayList<>(opts.directories().length);
    for (String source : opts.directories()) {
      sources.add(root.resolve(source));
    }

    // get each file in the staging area
    JavaPairRDD<String, PortableDataStream>[] stringRdds = new JavaPairRDD[sources.size()];
    for (int i = 0; i < sources.size(); i++) {
      stringRdds[i] = context.binaryFiles(sources.get(i).toString());
    }

    // transform each binary file into a bunch of avro generic records
    JavaRDD<GenericRecord>[] avroRecords = new JavaRDD[stringRdds.length];
    for (int i = 0; i < avroRecords.length; i++) {
      JavaPairRDD<String, PortableDataStream> content = stringRdds[i];
      avroRecords[i] = content.flatMap(tuple -> {
        String filename = tuple._1();
        PortableDataStream stream = tuple._2();
        FSDataInputStream in = (FSDataInputStream) stream.open();
        return () -> {
          try {
            return new Iterator<GenericRecord>() {
              MultiSchemaFileReader<GenericRecord> reader =
                new MultiSchemaFileReader<>(new SeekableDataInput(in));
              GenericRecord next;
              boolean exhausted = false;

              @Override
              public boolean hasNext() {
                if (next == null) {
                  tryNext();
                }
                exhausted = next == null;
                return exhausted;
              }

              @Override
              public GenericRecord next() {
                if (next == null) {
                  tryNext();
                }
                return next;
              }

              private void tryNext() {
                if (exhausted) {
                  return;
                }
                try {
                  next = reader.next(next);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            };
          } catch (IOException e) {
            throw new RuntimeException("Failed to open reader for file: " + filename, e);
          }
        };
      });
    }

    // combine into new single partition and remove duplicates
    JavaRDD<GenericRecord> records = context.union(avroRecords).distinct();

    // write to s3-glacier gzip
    //http://stackoverflow.com/questions/17241185/spark-standalone-mode-how-to-compress-spark
    // -output-written-to-hdfs
//    records.map
//    messages.saveAsTextFile(opts.archive(), GzipCodec.class);

    // map to [tenant, [...fields...]

    // get the current schema


    // separate out files that don't have a tenant id... how did these get here?
    //TODO send a cloudwatch notification with the count

    // dedup per company

    // commit schema back

    // write RS formatted files

    // write RS manifest
  }

  public void run(SparkContext context) {
    JavaSparkContext jc = new JavaSparkContext(context);

  }
}
