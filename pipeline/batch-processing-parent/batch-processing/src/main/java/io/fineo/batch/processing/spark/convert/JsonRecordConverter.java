package io.fineo.batch.processing.spark.convert;

import com.google.inject.Guice;
import io.fineo.lambda.configure.DefaultCredentialsModule;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.handle.raw.RawJsonToRecordHandler;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.lambda.kinesis.IKinesisProducer;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * Convert raw json events into avro typed records. This makes a large amount of database calls,
 * so you should probably checkpoint the RDD after complete to ensure we don't do it multiple times.
 */
public class JsonRecordConverter extends RecordConverter<Map<String, Object>>{

  public JsonRecordConverter(Properties props) {
    super(props);
  }

  @Override
  protected Map<String, Object> transform(Map<String, Object> obj) {
    return obj;
  }

}
