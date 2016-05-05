package io.fineo.batch.processing.lambda.sns.remote;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.inject.Inject;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.lambda.sns.SnsS3FileEventHandler;
import io.fineo.lambda.JsonParser;

import java.io.IOException;
import java.util.Map;

/**
 * Handles the sqs events where there was a write to Fineo's ingest S3 bucket
 */
public class SnsRemoteS3FileEventHandler extends SnsS3FileEventHandler {

  @Inject
  public SnsRemoteS3FileEventHandler(IngestManifest manifest) {
    super(manifest);
  }

  @Override
  protected RecordUpload parseOrgAndS3Location(SNSEvent.SNSRecord record)
    throws IOException {
    RecordUpload pair = getPair();
    SNSEvent.SNS event = record.getSNS();
    String user = event.getSubject();
    pair.setUser(user);

    String file = event.getMessage();
    pair.setLocation(file);
    return pair;
  }
}
