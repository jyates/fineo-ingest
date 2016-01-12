package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import io.fineo.lambda.avro.FirehoseClientProperties;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestAvroToDynamoWriter {

  @ClassRule
  public static AwsDynamoResource dynamo = new AwsDynamoResource();

  @Test
  public void testSingleWrite() throws Exception {
    dynamo.start();
    Properties prop = new Properties();
    dynamo.setConnectionProperties(prop);

    // setup the writer
    FirehoseClientProperties props = new FirehoseClientProperties(prop);
    dynamo.setCredentials(props);
    AvroToDynamoWriter writer = AvroToDynamoWriter.create(props);

    // create a basic record to write that is 'avro correct'
    GenericRecord record = SchemaTestUtils.createRandomRecord();

    // write it to dynamo and wait for a response
    writer.write(record);
    writer.flush();

    // ensure that the expected table got created
    AmazonDynamoDBClient client = dynamo.getClient();
    ListTablesResult tables = client.listTables();
    assertEquals(1, tables.getTableNames().size());

    // ensure that the record we wrote matches what we created

  }
}
