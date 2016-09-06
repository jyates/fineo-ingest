package io.fineo.lambda.e2e.state;

import org.apache.avro.generic.GenericRecord;

import java.util.Map;

/**
 * Track the various forms of the event that was sent.
 */
public class EventFormTracker {
  private byte[] sent;
  private Map<String, Object> json;
  private GenericRecord avro;
  private Map<String, Object> expected;

  public void sent(byte[] send) {
    this.sent = send;
  }

  public void sending(Map<String, Object> json) {
    this.json = json;
  }

  public byte[] getSent() {
    return sent;
  }

  public Map<String, Object> getJson() {
    return json;
  }

  public GenericRecord getAvro() {
    return avro;
  }

  public void setRecord(GenericRecord record) {
    this.avro = record;
  }

  public void expect(Map<String, Object> expectedOut) {
    this.expected = expectedOut;
  }

  public Map<String, Object> getExpected() {
    return expected;
  }
}
