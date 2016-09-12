package io.fineo.lambda.handle.ingest;

import java.util.Map;

public class CustomerEventRequest {

  private String customerKey;
  private Map<String, Object> event;
  private Map<String, Object>[] events;

  public String getCustomerKey() {
    return customerKey;
  }

  public CustomerEventRequest setCustomerKey(String customerKey) {
    this.customerKey = customerKey;
    return this;
  }

  public Map<String, Object> getEvent() {
    return event;
  }

  public CustomerEventRequest setEvent(Map<String, Object> event) {
    this.event = event;
    return this;
  }

  public Map<String, Object>[] getEvents() {
    return events;
  }

  public CustomerEventRequest setEvents(Map<String, Object>[] events) {
    this.events = events;
    return this;
  }
}
