package io.fineo.batch.processing.spark;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.Guice;
import com.google.inject.Module;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.handle.schema.SchemaStoreModuleForTesting;
import io.fineo.lambda.handle.schema.inject.DynamoDBRepositoryProvider;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import io.fineo.spark.avro.AvroSparkUtils;
import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

import static io.fineo.batch.processing.spark.options.BatchOptions.BATCH_ERRORS_OUTPUT_DIR_KEY;
import static io.fineo.lambda.configure.util.InstanceToNamed.property;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Batch processing against 'real' data, but using local resources
 */
@Category(AwsDependentTests.class)
public class TestBatchProcessorWithLocalResources {

  private static final String BUCKET = "test.fineo.io";
  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();
  @Rule
  public AwsDynamoTablesResource tables = new AwsDynamoTablesResource(dynamo);

  @ClassRule
  public final static AwsCredentialResource credentials = new AwsCredentialResource();
  private static final AWSCredentialsProvider CREDS = credentials.getProvider();

  @ClassRule
  public static LocalSparkRule spark = new LocalSparkRule(
    conf -> {
      AvroSparkUtils.setKyroAvroSerialization(conf);
    });

  @Rule
  public TestOutput output = new TestOutput(false);

  private List<Pair<String, String>> files = new ArrayList<>();

  @After
  public void cleanupS3() {
    AmazonS3Client s3 = credentials.getClient();
    for (Pair<String, String> file : files) {
      s3.deleteObject(file.getKey(), file.getValue());
    }
    files.clear();
  }

  @Test
  public void testBatchReadSingleRowS3CsvFile() throws Exception {
    File source = write("single_row", false,
      "id,obvius_upload_id,measurement,measured_at,unit,building_sensor_config_id,bad_data,"
      + "created_at,updated_at,metrictype"
      ,
      "7492,,652.304260253906,2015-10-24 04:00:00,,4685,f,2016-01-21 15:32:12.183183,2016-01-21 "
      + "15:32:12.183183,carbon_dioxide"
    );
    String path = upload(source);
    TestProperties props = runCarbonDioxideRead(path);
    validateTableRead(props, 1);
  }

  @Test
  public void testBatchReadShortS3CsvFile() throws Exception {
    // have to use s3n here because emr has the actual s3 jar we use in prod, but not available
    // publicly (screw you aws).
    File source = write("short", false,
      "id,obvius_upload_id,measurement,measured_at,unit,building_sensor_config_id,bad_data,"
      + "created_at,updated_at,metrictype"
      ,
      "7492,,652.304260253906,2015-10-24 04:00:00,,4685,f,2016-01-21 15:32:12.183183,2016-01-21 "
      + "15:32:12.183183,carbon_dioxide"
      ,
      "7500,,644.123229980469,2015-10-24 04:15:00,,4685,f,2016-01-21 15:32:12.199681,2016-01-21 "
      + "15:32:12.199681,carbon_dioxide"
      ,
      "7507,,652.304260253906,2015-10-24 04:30:00,,4685,f,2016-01-21 15:32:12.212263,2016-01-21 "
      + "15:32:12.212263,carbon_dioxide"
      ,
      "7513,,659.93994140625,2015-10-24 04:45:00,,4685,f,2016-01-21 15:32:12.226382,2016-01-21 "
      + "15:32:12.226382,carbon_dioxide"
    );
    TestProperties props = runCarbonDioxideRead(upload(source));
    validateTableRead(props, 4);
  }

  private String upload(File file) {
    AmazonS3Client s3 = credentials.getClient();
    String path = format("batch/%s", file.getName());
    s3.putObject(BUCKET, path, file);
    files.add(new ImmutablePair<>(BUCKET, path));
    // have to use s3n here because emr has the actual s3 jar we use in prod, but not available
    // publicly (screw you aws).
    return format("s3n://%s/%s", BUCKET, path);
  }

  private File write(String name, boolean zip, String... lines) throws IOException {
    File out = new File(output.newFolder(), name + ".csv" + (zip ? ".gz" : ""));
    try (OutputStream fos = new FileOutputStream(out);
         OutputStream fos2 = zip ? new GZIPOutputStream(fos) : fos;
         PrintWriter w = new PrintWriter(fos2)) {
      for (String line : lines) {
        w.println(line);
      }
    }
    return out;
  }

  private TestProperties runCarbonDioxideRead(String file)
    throws Exception {
    String org = "sadfsdfsdf3223gdnlfkas";
    int uuid = new Random().nextInt(100000);
    String dataTablePrefix = uuid + "-test-storage";
    String schemaStoreTable = uuid + "-test-schemaStore";
    Properties properties = new Properties();
    // create a metric in the metric store for our test org
    List<Module> modules = new ArrayList<>();
    modules.add(new SchemaStoreModuleForTesting());
    modules.add(instanceModule(tables.getAsyncClient()));
    modules.add(property(DynamoDBRepositoryProvider.DYNAMO_SCHEMA_STORE_TABLE, schemaStoreTable));
    SchemaStore store = Guice.createInjector(modules).getInstance(SchemaStore.class);
    StoreManager manager = new StoreManager(store);
    manager.newOrg(org)
           .newMetric().setDisplayName("carbon_dioxide")
           .withTimestampFormat("uuuu-MM-dd HH:mm:ss@ UTC-6")
           .newField().withName("id").withType(StoreManager.Type.LONG).build()
           .newField().withName("obvius_upload_id").withType(StoreManager.Type.LONG).build()
           .newField().withName("measurement").withType(StoreManager.Type.DOUBLE).build()
           .newField().withName("unit").withType(StoreManager.Type.VARCHAR).build()
           .newField().withName("bad_data").withType(StoreManager.Type.VARCHAR).build()
           .newField().withName("created_at").withType(StoreManager.Type.VARCHAR).build()
           .newField().withName("updated_at").withType(StoreManager.Type.VARCHAR).build()
           .newField().withName("building_sensor_config_id").withType(StoreManager.Type.INT).build()
           .build()
           .commit();
    // field aliases have to be added after creation of the metric
    manager.updateOrg(org).updateMetric("carbon_dioxide").addFieldAlias("timestamp", "measured_at")
           .build()
           .commit();

    LocalSparkOptions options =
      new LocalSparkOptions(dynamo.getUtil().getUrl(), dataTablePrefix, schemaStoreTable);
    options.setInput(new ImmutablePair<>(org, file));
    Properties props = new Properties();
    File errors = output.newFolder();
    props.setProperty(BATCH_ERRORS_OUTPUT_DIR_KEY, errors.getAbsolutePath());
    options.setProps(props);

    // setup s3 credentials
    Configuration conf = spark.jsc().hadoopConfiguration();
    conf.set("fs.s3n.awsAccessKeyId", CREDS.getCredentials().getAWSAccessKeyId());
    conf.set("fs.s3n.awsSecretAccessKey", CREDS.getCredentials().getAWSSecretKey());

    BatchProcessor processor = new BatchProcessor(options);
    processor.run(spark.jsc());

    TestProperties testProperties = new TestProperties();
    testProperties.setDataTablePrefix(dataTablePrefix);
    testProperties.setOrg(org);
    testProperties.setErrorFile(errors);
    return testProperties;
  }

  private void validateTableRead(TestProperties props, int numRows) {
    AmazonDynamoDBAsyncClient dynamo = tables.getAsyncClient();
    DynamoDB db = new DynamoDB(dynamo);
    TableCollection<ListTablesResult> tables = db.listTables(props.dataTablePrefix);
    Table table = null;
    for (Table t : tables) {
      if (t.getTableName().startsWith(props.dataTablePrefix)) {
        String msg = "Have an existing storage table: " + table + ", but found:" + t.getTableName();
        assertNull(msg, table);
        table = t;
      } else {
        break;
      }
    }
    long count = StreamSupport.stream(table.scan().spliterator(), false).count();
    assertEquals("Wrong number of rows read in dynamo!", numRows, count);
    assertFalse("Found errors files: " + Arrays.toString(props.errorFile.list()),
      props.errorFile.list().length > 0);
  }

  private class TestProperties {
    private String org;
    private String dataTablePrefix;
    private File errorFile;

    public String getOrg() {
      return org;
    }

    public void setOrg(String org) {
      this.org = org;
    }

    public String getDataTablePrefix() {
      return dataTablePrefix;
    }

    public void setDataTablePrefix(String dataTablePrefix) {
      this.dataTablePrefix = dataTablePrefix;
    }

    public void setErrorFile(File errorFile) {
      this.errorFile = errorFile;
    }
  }
}
