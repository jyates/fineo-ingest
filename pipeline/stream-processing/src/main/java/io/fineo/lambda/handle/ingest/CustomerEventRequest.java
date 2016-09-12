package io.fineo.lambda.handle.ingest;

import java.util.Map;

public class CustomerEventRequest {

  private String customerKey;
  private Map<String, Object> event;
  private Map<String, Object>[] events;

  public String getCustomerKey() {
    return customerKey;
  }

  public void setCustomerKey(String customerKey) {
    this.customerKey = customerKey;
  }

  public Map<String, Object> getEvent() {
    return event;
  }

  public void setEvent(Map<String, Object> event) {
    this.event = event;
  }

  public Map<String, Object>[] getEvents() {
    return events;
  }

  public void setEvents(Map<String, Object>[] events) {
    this.events = events;
  }
}
