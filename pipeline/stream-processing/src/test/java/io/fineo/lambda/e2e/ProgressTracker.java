package io.fineo.lambda.e2e;

import org.apache.avro.generic.GenericRecord;

import java.util.Map;


public class ProgressTracker {
  public byte[] sent;
  public Map<String, Object> json;
  public GenericRecord avro;

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
}
