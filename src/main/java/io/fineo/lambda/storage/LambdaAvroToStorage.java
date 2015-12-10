package io.fineo.lambda.storage;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import io.fineo.lambda.avro.FirehoseClientProperties;
import io.fineo.lambda.avro.FirehoseUtils;
import org.apache.avro.file.FirehoseRecordReader;

import java.io.IOException;

/**
 * Writes avro encoded files into the correct storage locations.
 * <p>
 * Avro files are written to two locations:
 * <ol>
 * <li>Firehose Staging: avro records are encoded one at a time and written to the configured
 * AWS Kinesis Firehose stream for 'staged' records. These are processed at a later time for
 * schema + bulk ingest. They can be processed with the standard
 * {@link FirehoseRecordReader}
 * </li>
 * <li>DynamoDB: Tables are created based on the timerange, with new tables created for every new
 * week as needed. See the dynamo writer for particular around table naming and schema.
 * </li>
 * </ol>
 * </p>
 */
public class LambdaAvroToStorage {

  private AmazonKinesisFirehoseClient firehoseClient;
  private FirehoseClientProperties props;
  private AmazonKinesisFirehoseClient firehose;
  private AvroToDynamoWriter dynamo;

  public void handler(KinesisEvent event) throws IOException {
    setup();
    handleEvent(event);
  }

  private void handleEvent(KinesisEvent event) throws IOException {
    //
  }

  private void setup() throws IOException {
    props = FirehoseClientProperties.load();

    this.firehose = FirehoseUtils.createFirehoseAndCheck(props,
      props.getFirehoseStagedStreamName());

    this.dynamo = AvroToDynamoWriter.create(props);
  }
}
