package io.fineo.batch.processing.spark;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.google.inject.Guice;
import com.google.inject.Module;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoResource;
import io.fineo.lambda.dynamo.rule.AwsDynamoTablesResource;
import io.fineo.lambda.handle.schema.SchemaStoreModuleForTesting;
import io.fineo.lambda.handle.schema.inject.DynamoDBRepositoryProvider;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import io.fineo.spark.avro.AvroSparkUtils;
import io.fineo.spark.rule.LocalSparkRule;
import io.fineo.test.rule.TestOutput;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import static io.fineo.batch.processing.spark.options.BatchOptions.BATCH_ERRORS_OUTPUT_DIR_KEY;
import static io.fineo.lambda.configure.util.InstanceToNamed.property;
import static io.fineo.lambda.configure.util.SingleInstanceModule.instanceModule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Batch processing against 'real' data, but using local resources
 */
@Category(AwsDependentTests.class)
public class TestBatchProcessorWithLocalResources {

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

  @Test
  public void testBatchReadSingleRowS3CsvFile() throws Exception {
    // have to use s3n here because emr has the actual s3 jar we use in prod, but not available
    // publicly (screw you aws).
    String file = "s3n://test.fineo.io/batch/carbon_dioxide_shorter.csv";
    TestProperties props = runCarbonDioxideRead(file);
    validateTableRead(props, 1);
  }

  @Test
  public void testBatchReadShortS3CsvFile() throws Exception {
    // have to use s3n here because emr has the actual s3 jar we use in prod, but not available
    // publicly (screw you aws).
    String file = "s3n://test.fineo.io/batch/carbon_dioxide_short.csv.gz";
    TestProperties props = runCarbonDioxideRead(file);
    validateTableRead(props, 4);
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
