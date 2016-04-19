package io.fineo.lambda.e2e;

import io.fineo.lambda.configure.legacy.StreamType;
import io.fineo.schema.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Track success for each phase of the {@link EndToEndTestRunner}
 */
public class EndtoEndSuccessStatus {
  private static final Log LOG = LogFactory.getLog(EndtoEndSuccessStatus.class);

  private boolean updated;
  private boolean messageSent;
  private List<Pair<String, StreamType>> correctFirehoses = new
    ArrayList<>();
  private boolean rawToAvro;
  private boolean avroToStorage;
  private boolean successful;

  public void updated() {
    this.updated = true;
  }

  public void sent() {
    this.messageSent = true;
  }

  public void firehoseStreamCorrect(String stream, StreamType type) {
    this.correctFirehoses.add(new Pair<>(stream, type));
  }

  public void rawToAvroPassed() {
    this.rawToAvro = true;
    LOG.info("Raw -> Avro stage succeeded!");
  }

  public void avroToStoragePassed() {
    this.avroToStorage = true;
    LOG.info("Avro -> Storage stage succeeded!");
  }

  /**
   * Setup check.
   * @return <tt>true</tt> if the schema store was updated with the correct information for the
   * record
   */
  public boolean isUpdateStoreCorrect() {
    return updated;
  }

  /**
   * Setup check
   * @return <tt>true</tt> if the manager reported that the message was sent successfully
   */
  public boolean isMessageSent() {
    return messageSent;
  }

  public List<Pair<String, StreamType>> getCorrectFirehoses() {
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
