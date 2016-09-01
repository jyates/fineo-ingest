package io.fineo.batch.processing.spark;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.google.inject.Guice;
import com.google.inject.Module;
import io.fineo.aws.AwsDependentTests;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.handle.schema.SchemaStoreModuleForTesting;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static io.fineo.lambda.configure.util.InstanceToNamed.property;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
  public static LocalSparkRule spark = new LocalSparkRule();
  @Rule
  public TestOutput output = new TestOutput(false);

  @Test
  public void testProcessJson() throws Exception {
    processFiles(1, json());
  }

  @Test
  public void testProcessCsv() throws Exception {
    processFiles(1, csv());
  }

  @Test
  public void testProcessJsonAndCsv() throws Exception {
    processFiles(2, csv(), json());
  }

  @Test
  public void testReadDifferentOrgFiles() throws Exception {
    processFiles(2, p("org1", csv()), p("org2", json()));
  }

  private String csv() throws IOException {
    File out = output.newFile();
    PrintWriter writer = new PrintWriter(out, "UTF-8");
    writer.println("metrictype,timestamp,field1");
    writer.println("\"metric\",1235,1");
    writer.close();
    return out.toString();
  }

  private String json() throws IOException {
    File out = output.newFile();
    PrintWriter w = new PrintWriter(out, "UTF-8");
    w.println("{");
    w.println(" \"metrictype\" : \"metric\",");
    w.println(" \"timestamp\" : 1234,");
    w.println(" \"field1\" : 1");
    w.println("}");
    w.close();
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

  private void processFiles(int numRows, Pair<String, String>... files) throws Exception {
    int uuid = new Random().nextInt(100000);
    String dataTablePrefix = uuid + "-test-storage";
    String schemaStoreTable = uuid + "-test-schemaStore";
    Properties properties = new Properties();
    properties.setProperty(FineoProperties.DYNAMO_REGION, REGION);
    // create a metric in the metric store for our test org
    List<Module> modules = new ArrayList<>();
    modules.add(new SchemaStoreModuleForTesting());
    modules.add(instanceModule(tables.getAsyncClient()));
    modules.add(property(SchemaStoreModule.DYNAMO_SCHEMA_STORE_TABLE,
      schemaStoreTable));
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
              throw new RuntimeException("Coudl not create org/metric for: " + org);
            }
          });


    LocalSparkOptions options =
      new LocalSparkOptions(dynamo.getUtil().getUrl(), dataTablePrefix,
        schemaStoreTable);
    withInput(options, files);

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
    assertNotNull("No data table found!", table);
    int count = 0;
    for (Item item : table.scan()) {
      count++;
    }
    assertEquals("Wrong number of rows in the data table", numRows, count);
  }

  private void withInput(LocalMockBatchOptions options,
    Pair<String, String>... fileInTestResources) {
    for (int i = 0; i < fileInTestResources.length; i++) {
        fileInTestResources[i].setValue("file://"+fileInTestResources[i].getValue());
      }
    options.setInput(fileInTestResources);
  }

}
