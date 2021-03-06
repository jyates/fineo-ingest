package io.fineo.batch.processing.spark;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Guice;
import com.google.inject.Module;

import io.fineo.aws.AwsDependentTests;
import io.fineo.batch.processing.dynamo.FailedIngestFile;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.handle.KinesisHandler;
import io.fineo.lambda.handle.schema.SchemaStoreModuleForTesting;
import io.fineo.lambda.handle.schema.inject.DynamoDBRepositoryProvider;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import io.fineo.spark.avro.AvroSparkUtils;
import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

import static io.fineo.batch.processing.spark.options.BatchOptions.BATCH_ERRORS_OUTPUT_DIR_KEY;
import static io.fineo.lambda.configure.util.InstanceToNamed.property;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Batch processor testing against local resources
 */
@Category(AwsDependentTests.class)
public class TestBatchProcessor {

  private static final String REGION = "us-east-1";
  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);
  @ClassRule
  public static LocalSparkRule spark = new LocalSparkRule(
      conf -> AvroSparkUtils.setKyroAvroSerialization(conf));
  @Rule
  public TestOutput output = new TestOutput(false);

  @Test
  public void testProcessJson() throws Exception {
    processFiles(1, json(false));
  }

  @Test
  public void testProcessGzipJson() throws Exception {
    processFiles(1, json(true));
  }

  @Test
  public void testProcessCsv() throws Exception {
    processFiles(1, csv(false));
  }

  @Test
  public void testProcessGzipCsv() throws Exception {
    processFiles(1, csv(true));
  }

  @Test
  public void testProcessJsonAndCsv() throws Exception {
    processFiles(2, csv(false), json(false));
  }

  @Test
  public void testReadDifferentOrgFiles() throws Exception {
    processFiles(2, p("org1", csv(false)), p("org2", json(false)));
  }

  @Test
  public void testFailToReadFile() throws Exception {
    String file = "not/a/file.json", org = "org";
    LocalSparkOptions options = processFiles(0, p(org, file));
    IngestManifest manifest = options.getManifest();
    assertTrue("Manifest not empty, still has: " + manifest.files(), manifest.files().isEmpty());
    Multimap<String, FailedIngestFile> failures = ArrayListMultimap.create();
    FailedIngestFile fail =
        new FailedIngestFile(org, "file://" + file, "File /a/file.json does not exist");
    failures.put(org, fail);
    assertEquals(failures, manifest.failures(false));
  }

  @Test
  public void testBadRecord() throws Exception {
    String org = "jorg1234";
    // missing metrictype
    Map<String, Object> event = new HashMap<>();
    event.put(AvroSchemaProperties.TIMESTAMP_KEY, 1234);

    String file = write("bad-data.json", false, writer -> {
      String json = Jackson.toJsonString(event);
      writer.write(json);
    });
    LocalSparkOptions options = processFiles(0, true, p(org, file));
    String base = options.getProperties().getProperty(BATCH_ERRORS_OUTPUT_DIR_KEY);
    LocalBatchErrors errors = new LocalBatchErrors(base);
    List<Instant> runs = errors.getRuns();
    assertEquals("Got runs: " + runs + ", under " + base, 1, runs.size());
    List<LocalBatchErrors.OrgErrors> orgErrors = errors.getErrors(runs.get(0));
    assertEquals("Wrong number of orgs with errors under " + base, 1, orgErrors.size());
    LocalBatchErrors.OrgErrors oe = orgErrors.get(0);
    assertEquals("Wrong number of errors for org under " + base, 1, oe.getErrors().size());
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> error =
        mapper.readValue(oe.getErrors().get(0), new TypeReference<Map<String, Object>>() {
        });

    String cosmeticCause = "Failed to apply schema for record";
    String rootCause = "No metric type found in record for metric type keys: [] or standard "
                       + "type key 'metrictype'";
    List<Map<String, Object>> causes = new ArrayList<>();
    causes.add(getErrorCause(cosmeticCause));
    causes.add(getErrorCause(rootCause));
    Map<String, Object> expected = new HashMap<>();
    expected.put("apikey", org);
    expected.put("message", cosmeticCause);
    expected.put("event", mapper.writeValueAsString(event));

    assertEquals(expected.get("apikey"), error.get("apikey"));
    assertTrue(((String) error.get("message")).startsWith((String) expected.get("message")));
//    List<Object> actualCauses = (List<Object>) error.get("causes");
//    assertEquals("Wrong call stack height!\nExpected causes:" + causes + "\nActual "
//                 + "Causes:" + actualCauses, causes.size(), actualCauses.size());
//    for (int i = 0; i < causes.size(); i++) {
//      if (i == 0) {
//        // top message should just be prefix of the real message
//        String cause = (String) causes.get(i).get("message");
//        cause += ": "+causes.get(1).get("message");
//        assertEquals("Mismatch for cause at depth: " + i, cause,
//            ((Map<String, Object>) actualCauses.get(i)).get("message"));
//        continue;
//      }
//      assertEquals("Mismatch for cause at depth: " + i,
//          causes.get(i).get("message"),
//          ((Map<String, Object>) actualCauses.get(i)).get("message"));
//    }
  }

  private Map<String, Object> getErrorCause(String message) {
    Map<String, Object> cause = new HashMap<>();
    cause.put("message", message);
    return cause;
  }

  private String csv(boolean zip) throws IOException {
    return write("test.csv", zip, writer -> {
      writer.println("metrictype,timestamp,field1");
      writer.println("\"metric\",1235,1");
    });
  }

  private String json(boolean zip) throws IOException {
    return write("test.json", zip, w -> {
      Map<String, Object> map = new HashMap<>();
      map.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, "metric");
      map.put(AvroSchemaProperties.TIMESTAMP_KEY, 1234);
      map.put("field1", 1);
      String msg;
      try {
        msg = new ObjectMapper().writeValueAsString(map);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      w.println(msg);
    });
  }

  private String write(String name, boolean zip, Consumer<PrintWriter> write) throws IOException {
    File folder = output.newFolder();
    if (zip) {
      name = name + ".gz";
    }
    File out = new File(folder, name);
    try (OutputStream fos = new FileOutputStream(out);
         OutputStream fos2 = zip ? new GZIPOutputStream(fos) : fos;
         PrintWriter w = new PrintWriter(fos2)) {
      write.accept(w);
    }
    return out.toString();
  }

  private <T, V> Pair<T, V> p(T t, V v) {
    return new MutablePair<>(t, v);
  }

  private void processFiles(int numRows, String... files) throws Exception {
    List<Pair<String, String>> mapped = new ArrayList<>();
    for (String file : files) {
      mapped.add(p("local", file));
    }
    processFiles(numRows, mapped.toArray(new Pair[0]));
  }

  private LocalSparkOptions processFiles(int numRows, Pair<String, String>... files)
      throws Exception {
    return processFiles(numRows, false, files);
  }

  private LocalSparkOptions processFiles(int numRows, boolean expectErrors,
      Pair<String, String>... files) throws Exception {
    int uuid = new Random().nextInt(100000);
    String dataTablePrefix = uuid + "-test-storage";
    String schemaStoreTable = uuid + "-test-schemaStore";
    // create a metric in the metric store for our test org
    List<Module> modules = new ArrayList<>();
    modules.add(new SchemaStoreModuleForTesting());
    modules.add(instanceModule(tables.getAsyncClient()));
    modules.add(property(DynamoDBRepositoryProvider.DYNAMO_SCHEMA_STORE_TABLE, schemaStoreTable));
    SchemaStore store = Guice.createInjector(modules).getInstance(SchemaStore.class);
    StoreManager manager = new StoreManager(store);
    Arrays.asList(files).stream()
          .map(p -> p.getKey())
          .distinct()
          .forEach(org -> {
            try {
              manager.newOrg(org).newMetric().setDisplayName("metric").newField().withName("field1")
                     .withType(StoreManager.Type.INTEGER).build().build().commit();
            } catch (Exception e) {
              throw new RuntimeException("Couldn't create org/metric for: " + org);
            }
          });

    LocalSparkOptions options =
        new LocalSparkOptions(dynamo.getUtil().getUrl(), dataTablePrefix, schemaStoreTable);
    withInput(options, files);
    Properties props = new Properties();
    File errors = output.newFolder();
    props.setProperty(BATCH_ERRORS_OUTPUT_DIR_KEY, errors.getAbsolutePath());
    options.setProps(props);

    BatchProcessor processor = new BatchProcessor(options);
    processor.run(spark.jsc());

    // validate the output
    AmazonDynamoDBAsyncClient dynamo = tables.getAsyncClient();
    DynamoDB db = new DynamoDB(dynamo);
    TableCollection<ListTablesResult> tables = db.listTables(dataTablePrefix);
    Table table = null;
    for (Table t : tables) {
      if (t.getTableName().startsWith(dataTablePrefix)) {
        String msg = "Have an existing storage table: " + table + ", but found:" + t.getTableName();
        assertNull(msg, table);
        table = t;
      } else {
        break;
      }
    }
    if (numRows > 0) {
      assertNotNull("No data table found!", table);
      long count = StreamSupport.stream(table.scan().spliterator(), false).count();
      assertEquals("Wrong number of rows in the data table", numRows, count);
    } else {
      assertNull("Shouldn't have written any data, but found data table: " + table, table);
    }

    assertEquals("Found errors files: " + Arrays.toString(errors.list()),
        expectErrors, errors.list().length > 0);

    return options;
  }

  private void withInput(LocalSparkOptions options,
      Pair<String, String>... fileInTestResources) {
    for (int i = 0; i < fileInTestResources.length; i++) {
      fileInTestResources[i].setValue("file://" + fileInTestResources[i].getValue());
    }
    options.setInput(fileInTestResources);
  }

}
