package io.fineo.lambda.e2e.state;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.ArrayUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Track the various forms of the event that was sent.
 */
public class EventFormTracker {
  private List<byte[]> sent = new ArrayList<>();
  private List<Map<String, Object>> json = new ArrayList<>();
  private List<GenericRecord> avro = new ArrayList<>();
  private List<Map<String, Object>> expected = new ArrayList<>();

  public void sent(byte[] send) {
    this.sent.add(send);
  }

  public void sending(Map<String, Object> json) {
    this.json.add(json);
  }

  public byte[] getSent() {
//    int size = sent.stream().mapToInt(bytes -> bytes.length).sum();
    byte[] out = new byte[0];
    for (byte[] b : sent){
      out = ArrayUtils.addAll(out, b);
    }

    return out;
  }

  public List<Map<String, Object>> getJson() {
    return json;
  }

  public List<GenericRecord> getAvro() {
    return avro;
  }

  public void setRecord(GenericRecord record) {
    this.avro.add(record);
  }

  public void expect(Map<String, Object> expectedOut) {
    this.expected.add(expectedOut);
  }

  public List<Map<String, Object>> getExpected() {
    return expected;
  }
}
