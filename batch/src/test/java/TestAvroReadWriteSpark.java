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

import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.store.SchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.file.MultiSchemaFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;

/**
 * Simple test class that ensures we can read/write avro files from spark
 */
public class TestAvroReadWriteSpark {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    // create a simple record
    AvroSchemaInstanceBuilder builder = new AvroSchemaInstanceBuilder();
    Schema s = builder.withName("ns").withName("r1")
                      .newField().name("f1").type("string").done()
                      .build();
    GenericRecord record = new GenericRecordBuilder(s).set("f1", "v1").set(SchemaBuilder
      .UNKNOWN_KEYS_FIELD, new HashMap<>()).build();

    // save the record to a file
    File file = folder.newFile();
    FileOutputStream fout = new FileOutputStream(file);
    MultiSchemaFileWriter writer = new MultiSchemaFileWriter(new GenericDatumWriter<>());
    writer.create();
    writer.append(record);
    fout.write(writer.close());
    fout.close();

    // open the file in spark and get the instances
  }

  private File getFile(String testPath) {
    ClassLoader classLoader = getClass().getClassLoader();
    return new File(classLoader.getResource(testPath).getFile());
  }
}
