package io.fineo.etl.processing.raw;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.iot.AWSIotAsyncClient;
import com.amazonaws.services.iot.model.AttributePayload;
import com.amazonaws.services.iot.model.UpdateThingRequest;
import com.amazonaws.services.iot.model.UpdateThingResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import io.fineo.etl.processing.OutputWriter;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import io.fineo.lambda.aws.MultiWriteFailures;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

/**
 * Writes avro events to the correct destination for the next stage of consumption
 */
public class AvroConvertedOutputWriter implements OutputWriter<GenericRecord> {

  private final AwsAsyncSubmitter<UpdateThingRequest, UpdateThingResult, GenericRecord> submitter;
  private final AWSIotAsyncClient iot;

  public AvroConvertedOutputWriter(AWSCredentialsProvider credentialsProvider, int retries) {
    this.iot = new AWSIotAsyncClient(credentialsProvider);
    this.submitter = new AwsAsyncSubmitter<>(retries, iot::updateThingAsync);
  }

  @Override
  public void write(GenericRecord obj) {
    AttributePayload payload = new AttributePayload();
    // serialize the object into a payload
  }

  @Override
  public MultiWriteFailures<GenericRecord> commit() {
    return this.submitter.flush();
  }
}
