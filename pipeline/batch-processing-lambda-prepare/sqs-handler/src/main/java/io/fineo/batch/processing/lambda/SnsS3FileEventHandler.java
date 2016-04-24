package io.fineo.batch.processing.lambda;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import io.fineo.batch.processing.dynamo.DynamoIngestManifest;
import io.fineo.lambda.handle.LambdaHandler;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import io.fineo.lambda.JsonParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Handles the sqs events where there was a write to Fineo's ingest S3 bucket
 */
public abstract class SnsS3FileEventHandler implements LambdaHandler<SNSEvent> {

  private final DynamoIngestManifest manifest;
  protected final JsonParser parser;
  private RecordUpload pair;

  public SnsS3FileEventHandler(DynamoIngestManifest manifest, JsonParser parser) {
    this.manifest = manifest;
    this.parser = parser;
  }

  @Override
  public void handle(SNSEvent event) throws IOException {
    List<SNSEvent.SNSRecord> records = event.getRecords();
    for (SNSEvent.SNSRecord record : records) {
      RecordUpload orgLocation = parseOrgAndS3Location(record);
      manifest.add(orgLocation.getUser(), orgLocation.getLocation());
    }
    manifest.flush();
  }



  protected abstract RecordUpload parseOrgAndS3Location(SNSEvent.SNSRecord record)
    throws IOException;

  protected RecordUpload getPair() {
    if (this.pair == null) {
      this.pair = new RecordUpload();
    }
    return this.pair;
  }

  protected class RecordUpload{
    private String user;
    private String location;

    public void setUser(String user) {
      this.user = user;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public String getUser() {
      return user;
    }

    public String getLocation() {
      return location;
    }
  }
}
