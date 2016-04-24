package io.fineo.batch.processing.lambda;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.inject.Inject;
import io.fineo.batch.processing.dynamo.DynamoIngestManifest;
import io.fineo.lambda.JsonParser;

import java.io.IOException;
import java.util.Map;

/**
 * Handles the sqs events where there was a write to Fineo's ingest S3 bucket
 */
public class SnsRemoteS3FileEventHandler extends SnsS3FileEventHandler {

  @Inject
  public SnsRemoteS3FileEventHandler(DynamoIngestManifest manifest, JsonParser parser) {
    super(manifest, parser);
  }

  @Override
  protected RecordUpload parseOrgAndS3Location(SNSEvent.SNSRecord record)
    throws IOException {
    RecordUpload pair = getPair();
    String msg = record.getSNS().getMessage();
    Map<String, Object> json = parser.parse(msg).iterator().next();
    String file = (String) json.get("file");
    pair.setLocation(file);

    // get the user somehow

    return pair;
  }
}
