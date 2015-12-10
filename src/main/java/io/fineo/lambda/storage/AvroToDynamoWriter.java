package io.fineo.lambda.storage;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import io.fineo.lambda.avro.FirehoseClientProperties;
import org.apache.avro.generic.GenericRecord;

/**
 * Write avro encoded records into dynamo
 * <p>
 * The name of the table is: [configured prefix]_[start time]-[end time] where start/end are
 * simply linux timestamps (names required to match pattern: [a-zA-Z0-9_.-]+).
 *
 * </p>
 * <p>
 * The schema of the table is as follows:
 * <table>
 *   <tr><th>Partition Key</th><th>Sort Key</th><th>Known Fields</th><th>Unknown Fields</th></tr>
 *   <tr><td>[orgID][schemaType]</td><td>[timestamp]</td><td>encoded with type</td><td>string
 *   encoded</td></tr>
 * </table>
 *
 * However, you should use a higher level api read and access records in Dynamo.
 * </p>
 */
public class AvroToDynamoWriter {
  private final AmazonDynamoDBClient client;
  private final String prefix;

  public AvroToDynamoWriter(AmazonDynamoDBClient client, String dynamoIngestTablePrefix) {
    this.client = client;
    this.prefix = dynamoIngestTablePrefix;
  }

  public static AvroToDynamoWriter create(FirehoseClientProperties props) {
    AmazonDynamoDBClient client = props.getDynamo();
    return new AvroToDynamoWriter(client, props.getDynamoIngestTablePrefix());
  }

  public void write(GenericRecord reuse) {

  }

  public void flush() {

  }
}
