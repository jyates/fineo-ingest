package io.fineo.etl.spark.read;

public class PartitionKey {

  private String org;
  private String metricId;

  public void setOrg(String org) {
    this.org = org;
  }

  public void setMetricId(String metricId) {
    this.metricId = metricId;
  }

  public String getOrg() {
    return org;
  }

  public String getMetricId() {
    return metricId;
  }

  public PartitionKey(){
  }

  public PartitionKey(PartitionKey other) {
    this.org = other.org;
    this.metricId = other.metricId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof PartitionKey))
      return false;

    PartitionKey that = (PartitionKey) o;

    if (getOrg() != null ? !getOrg().equals(that.getOrg()) : that.getOrg() != null)
      return false;
    return getMetricId() != null ? getMetricId().equals(that.getMetricId()) :
           that.getMetricId() == null;

  }

  @Override
  public int hashCode() {
    int result = getOrg() != null ? getOrg().hashCode() : 0;
    result = 31 * result + (getMetricId() != null ? getMetricId().hashCode() : 0);
    return result;
  }
}
