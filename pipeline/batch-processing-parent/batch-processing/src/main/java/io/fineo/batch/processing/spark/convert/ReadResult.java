package io.fineo.batch.processing.spark.convert;

import java.io.Serializable;

/**
 * Result of reading an event from a file
 */
public class ReadResult implements Serializable{

  private Outcome out;
  private String org;

  public enum Outcome{
    SUCCESS,
    FAILURE
  }

  public ReadResult(Outcome out, String org) {
    this.out = out;
    this.org = org;
  }

  public Outcome getOut() {
    return out;
  }

  public String getOrg() {
    return org;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof ReadResult))
      return false;

    ReadResult that = (ReadResult) o;

    if (getOut() != that.getOut())
      return false;
    if (!getOrg().equals(that.getOrg()))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = getOut().hashCode();
    result = 31 * result + getOrg().hashCode();
    return result;
  }
}
