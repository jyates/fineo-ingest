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

import com.google.common.collect.AbstractIterator;
import io.fineo.etl.options.ETLOptionBuilder;
import io.fineo.etl.options.ETLOptions;
import io.fineo.schema.avro.RecordMetadata;
import org.apache.avro.file.FirehoseRecordReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class SparkETL {

  private final ETLOptions opts;

  public SparkETL(ETLOptions opts) {
    this.opts = opts;
  }

  public void run(JavaSparkContext context) throws URISyntaxException, IOException {
    // find all the files under the given root directory
    URI root = new URI(opts.root());
    FileSystem fs = FileSystem.get(root, context.hadoopConfiguration());
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

    // transform each binary file into a bunch of avro generic records
    JavaRDD<GenericRecord>[] avroRecords = new JavaRDD[stringRdds.length];
    for (int i = 0; i < avroRecords.length; i++) {
      JavaPairRDD<String, PortableDataStream> content = stringRdds[i];
      avroRecords[i] = content.flatMap(new RecordMapper());
    }

    // combine into new single partition and remove duplicates
    JavaRDD<GenericRecord> records = context.union(avroRecords).distinct();
    records.map(record -> {
      RecordMetadata metadata = RecordMetadata.get(record);
      metadata.getOrgID()
    })

    // map to [tenant, [...fields...]

    // get the current schema


    // separate out files that don't have a tenant id... how did these get here?
    //TODO send a cloudwatch notification with the count

    // dedup per company

    // commit schema back

    // write RS formatted files

    // write RS manifest
  }

  private static class RecordMapper
    implements FlatMapFunction<Tuple2<String, PortableDataStream>, GenericRecord> {
    @Override
    public Iterable<GenericRecord> call(
      Tuple2<String, PortableDataStream> tuple) throws Exception {
      PortableDataStream stream = tuple._2();
      FSDataInputStream in = (FSDataInputStream) stream.open();
      GenericRecordReader reader = new GenericRecordReader(in);
      return () -> reader;
    }
  }

  private static class GenericRecordReader extends AbstractIterator<GenericRecord> {

    private final FirehoseRecordReader<GenericRecord> reader;

    private GenericRecordReader(FSDataInputStream in) throws Exception {
      this.reader = FirehoseRecordReader.create(new AvroFSInput(in, in.available()));
    }

    @Override
    protected GenericRecord computeNext() {
      try {
        GenericRecord next = reader.next();
        if (next == null) {
          endOfData();
        }
        return next;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) throws URISyntaxException, IOException {
    // parse arguments
    ETLOptions opts = ETLOptionBuilder.build(args);
    if (opts.help() || opts.error()) {
      opts.printHelp();
      System.exit(opts.error() ? 1 : 0);
    }

    SparkETL etl = new SparkETL(opts);
    SparkConf conf = new SparkConf().setAppName(SparkETL.class.getName());
    final JavaSparkContext context = new JavaSparkContext(conf);
    etl.run(context);
  }
}
