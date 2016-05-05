package io.fineo.batch.processing.lambda.sns;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import io.fineo.batch.processing.dynamo.IngestManifest;
import io.fineo.lambda.handle.LambdaHandler;

import java.io.IOException;
import java.util.List;

/**
 * Handles the sqs events where there was a write to Fineo's ingest S3 bucket
 */
public abstract class SnsS3FileEventHandler implements LambdaHandler<SNSEvent> {

  private final IngestManifest manifest;
  private RecordUpload pair;

  public SnsS3FileEventHandler(IngestManifest manifest) {
    this.manifest = manifest;
  }

  @Override
  public void handle(SNSEvent event) throws IOException {
    List<SNSEvent.SNSRecord> records = event.getRecords();
    boolean addedEvents = false;
    for (SNSEvent.SNSRecord record : records) {
      RecordUpload orgLocation = parseOrgAndS3Location(record);
      manifest.add(orgLocation.getUser(), orgLocation.getLocation());
      addedEvents = true;
    }
    if (addedEvents) {
      manifest.flush();
    }
  }

  protected abstract RecordUpload parseOrgAndS3Location(SNSEvent.SNSRecord record)
    throws IOException;

  protected RecordUpload getPair() {
    if (this.pair == null) {
      this.pair = new RecordUpload();
    }
    return this.pair;
  }

  protected class RecordUpload {
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

    @Override
    public String toString() {
      return "RecordUpload{" +
             "user='" + user + '\'' +
             ", location='" + location + '\'' +
             '}';
    }
  }
}
