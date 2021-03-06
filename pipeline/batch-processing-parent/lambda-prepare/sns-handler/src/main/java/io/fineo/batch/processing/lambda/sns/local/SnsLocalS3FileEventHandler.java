package io.fineo.batch.processing.lambda.sns.local;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.google.inject.Inject;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.batch.processing.lambda.sns.SnsS3FileEventHandler;
import io.fineo.lambda.JsonParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Handles the sqs events where there was a write to Fineo's ingest S3 bucket
 */
public class SnsLocalS3FileEventHandler extends SnsS3FileEventHandler {

  private final JsonParser parser;

  @Inject
  public SnsLocalS3FileEventHandler(IngestManifest manifest, JsonParser parser) {
    super(manifest);
    this.parser = parser;
  }

  @Override
  protected RecordUpload parseOrgAndS3Location(SNSEvent.SNSRecord record)
    throws IOException {
    RecordUpload pair = getPair();
    String msg = record.getSNS().getMessage();
    Map<String, Object> json = parser.parse(msg).iterator().next();
    List<Map<String, Object>> records = (List<Map<String, Object>>) json.get("Records");
    Map<String, Object> s3 = (Map<String, Object>) records.get(0).get("s3");
    Map<String, Object> bucket = (Map<String, Object>) s3.get("bucket");
    String bucketName = (String) bucket.get("name");
    Map<String, String> object = (Map<String, String>) s3.get("object");
    String key = object.get("key");
    String[] parts = key.split("/");
    String name = parts[1];
    String id = name.split("_")[0];
    pair.setUser(id);

    String s3Location = "s3://" + bucketName + "/" + key;
    pair.setLocation(s3Location);
    return pair;
  }

}
