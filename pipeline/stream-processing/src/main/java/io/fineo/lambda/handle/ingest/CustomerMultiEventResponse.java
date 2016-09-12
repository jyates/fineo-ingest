package io.fineo.lambda.handle.ingest;

import java.util.List;

public class CustomerMultiEventResponse extends CustomerEventResponse {
  private List<EventResult> results;

  public List<EventResult> getResults() {
    return results;
  }

  public CustomerMultiEventResponse setResults(List<EventResult> results) {
    this.results = results;
    return this;
  }
}
