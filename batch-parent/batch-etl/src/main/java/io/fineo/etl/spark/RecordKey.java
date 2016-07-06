package io.fineo.etl.spark;

import io.fineo.internal.customer.BaseFields;
import io.fineo.schema.avro.RecordMetadata;

import java.io.Serializable;
import java.sql.Date;

/**
 * Key for GenericRecords. Enables just tracking the 'unknown' fields assigned to this org/metric.
 */
public class RecordKey implements Serializable {

  private final String org;
  private final String metricId;
  private final Date date;
  private boolean unknownFields = false;
  private transient RecordMetadata metadata;

  public RecordKey(RecordMetadata metadata) {
    this.org = metadata.getOrgID();
    this.metricId = metadata.getMetricCanonicalType();
    this.date = new Date(metadata.getBaseFields().getTimestamp());
    this.metadata = metadata;
  }

  public RecordKey unknown() {
    BaseFields fields = metadata.getBaseFields();
    if (fields.getUnknownFields().size() == 0) {
      return null;
    } else {
      RecordKey key = new RecordKey(this);
      key.unknownFields = true;
      return key;
    }
  }

  public RecordKey known() {
    return this;
  }

  public RecordKey(RecordKey other) {
    this.org = other.org;
    this.metricId = other.metricId;
    this.date = other.date;
    this.metadata = other.metadata;
    this.unknownFields = other.unknownFields;
  }

  public boolean isUnknown() {
    return this.unknownFields;
  }

  public String getMetricId() {
    return this.metricId;
  }

  public String getOrgId() {
    return this.org;
  }

  public Date getDate() {
    return this.date;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof RecordKey))
      return false;

    RecordKey recordKey = (RecordKey) o;

    if (unknownFields != recordKey.unknownFields)
      return false;
    if (!org.equals(recordKey.org))
      return false;
    if (!getMetricId().equals(recordKey.getMetricId()))
      return false;
    return getDate().equals(recordKey.getDate());

  }

  @Override
  public int hashCode() {
    int result = org.hashCode();
    result = 31 * result + getMetricId().hashCode();
    result = 31 * result + getDate().hashCode();
    result = 31 * result + (unknownFields ? 1 : 0);
    return result;
  }
}
