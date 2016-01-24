package io.fineo.lambda.dynamo;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;

import java.time.Period;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Read records from Dynamo
 */
public class AvroDynamoReader {

  private final AmazonDynamoDBAsyncClient client;
  private final SchemaStore store;

  public AvroDynamoReader(SchemaStore store, AmazonDynamoDBAsyncClient client){
    this.store = store;
    this.client = client;
  }

  public Iterable<GenericRecord> scan(String orgId, String aliasMetricName, Period range){
    Metadata org = store.getSchemaTypes(orgId);
    Metric metric = store.getMetricMetadataFromAlias(org, aliasMetricName);

    // get the potential tables that match the range

    // get a scan across each table
    List<ScanRequest> requests = new ArrayList<>();

    // create an iterable around all the requests
    return new Iterable<GenericRecord>() {
      @Override
      public Iterator<GenericRecord> iterator() {
        AtomicReference<Exception> exceptions = new AtomicReference<>();
        CountDownLatch completed = new CountDownLatch(requests.size());
        // run each request in parallel, load them as they are present
        for(ScanRequest request: requests){
          client.scanAsync(request, new AsyncHandler<ScanRequest, ScanResult>() {
            @Override
            public void onError(Exception exception) {
              exceptions.set(exception);
            }

            @Override
            public void onSuccess(ScanRequest request, ScanResult scanResult) {

            }
          })
        }
        return null;
      }
    };
  }
}