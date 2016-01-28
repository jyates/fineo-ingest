package io.fineo.lambda.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Track success for each phase of the {@link EndToEndTestRunner}
 */
public class EndtoEndSuccessStatus {
  private boolean updated;
  private boolean messageSent;
  private List<String> correctFirehoses = new ArrayList<>();
  private boolean rawToAvro;
  private boolean avroToStorage;
  private boolean successful;

  public void updated() {
    this.updated = true;
  }

  public void sent() {
    this.messageSent = true;
  }

  public void firehoseStreamCorrect(String stream) {
    this.correctFirehoses.add(stream);
  }

  public void rawToAvroPassed() {
    this.rawToAvro = true;
  }

  public void avroToStoragePassed() {
    this.avroToStorage = true;
  }

  public boolean isUpdateStoreCorrect() {
    return updated;
  }

  public boolean isMessageSent() {
    return messageSent;
  }

  public List<String> getCorrectFirehoses() {
    return correctFirehoses;
  }

  public boolean isRawToAvroSuccessful() {
    return rawToAvro;
  }

  public boolean isAvroToStorageSuccessful() {
    return avroToStorage;
  }

  public void success() {
    this.successful = true;
  }

  public boolean isSuccessful() {
    return successful;
  }
}