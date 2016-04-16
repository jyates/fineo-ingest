package io.fineo.etl.processing;

import org.apache.avro.generic.GenericRecord;

/**
 * Wrapper for a message to write
 */
public class Message<T> {

  private final T info;
  private final GenericRecord record;

  public Message(T info, GenericRecord record){
    this.info = info;
    this.record = record;
  }

  public T getInfo() {
    return info;
  }

  public GenericRecord getRecord() {
    return record;
  }
}
