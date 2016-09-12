package io.fineo.lambda.handle;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.fineo.lambda.configure.util.SingleInstanceModule;
import io.fineo.lambda.handle.ingest.CustomerEventHandler;
import io.fineo.lambda.handle.ingest.CustomerEventIngest;
import io.fineo.lambda.handle.ingest.CustomerEventRequest;
import io.fineo.lambda.handle.ingest.CustomerEventResponse;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.schema.store.AvroSchemaProperties;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.configure.util.InstanceToNamed.namedInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

public class TestClientToRawStream {

  private final String orgId = "orgId";
  private final String stream = "stream-name";

  @Test
  public void testSingleEvent() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Multimap<String, byte[]> events = ArrayListMultimap.create();

    AmazonKinesisAsyncClient client = getStreamToMapClient(events);
    CustomerEventIngest ingest = getLambda(mapper, stream, client);
    CustomerEventRequest request = new CustomerEventRequest();

    Map<String, Object> event = new HashMap<>();
    event.put(AvroSchemaProperties.TIMESTAMP_KEY, 1234);
    request.setCustomerKey(orgId);
    request.setEvent(event);
    ingest.handle(request, Mockito.mock(Context.class));

    verifyReadEvents(events, mapper, stream, event);
  }


  @Test
  public void testFromRawJson() throws Exception {
    Map<String, Object> event = new HashMap<>();
    event.put(AvroSchemaProperties.TIMESTAMP_KEY, 124);
    Map<String , Object> map = new HashMap<>();
    map.put("customerKey", orgId);
    map.put("event", event);
    String msg = Jackson.toJsonPrettyString(map);
    CustomerEventRequest request = Jackson.fromJsonString(msg, CustomerEventRequest.class);
    assertEquals(orgId, request.getCustomerKey());
    assertEquals(event, request.getEvent());
    assertNull(request.getEvents());
  }
  
  @Test
  public void testEvents() throws Exception {
    Map<String, Object> o1 = new HashMap<>();
    o1.put(AvroSchemaProperties.TIMESTAMP_KEY, 1);

    writeEvents(o1);
  }

  @Test
  public void testMultipleEvents() throws Exception {
    Map<String, Object> o1 = new HashMap<>();
    o1.put(AvroSchemaProperties.TIMESTAMP_KEY, 1);

    Map<String, Object> o2 = new HashMap<>();
    o2.put(AvroSchemaProperties.TIMESTAMP_KEY, 2);
    o2.put("field", "fsdfsd");
    writeEvents(o1, o2);
  }

  @Test
  public void testTooManyRecords() throws Exception {
    Map[] events = new Map[501];
    for (int i = 0; i < events.length; i++) {
      events[i] = new HashMap<>();
    }

    Context context = context();
    CustomerEventIngest ingest = getLambda(new ObjectMapper(), stream, getStreamToMapClient(null));
    try {
      ingest.handle(new CustomerEventRequest().setCustomerKey(orgId).setEvents(events), context);
      fail();
    } catch (Exception e) {
      HandlerTestUtils.expectError(e, 400, "Bad Request");
    }
  }

  @Test
  public void testMissingTimestampEvent() throws Exception {
    Context context = context();
    CustomerEventIngest ingest = getLambda(new ObjectMapper(), stream, getStreamToMapClient(null));
    try {
      ingest
        .handle(new CustomerEventRequest().setCustomerKey(orgId).setEvent(new HashMap<>()),
          context);
      fail();
    }catch (Exception e){
      HandlerTestUtils.expectError(e, 400, "Bad Request");
    }
  }

  @Test
  public void testIncludesEmailStacktraceForMissingOrgId() throws Exception {
    CustomerEventIngest ingest = getLambda(new ObjectMapper(), stream, getStreamToMapClient(null));
    HandlerTestUtils.fail500(prov -> ingest.getInstance(), new CustomerEventRequest());
  }

  private Context context() {
    Context context = Mockito.mock(Context.class);
    Mockito.when(context.getAwsRequestId()).thenReturn(UUID.randomUUID().toString());
    return context;
  }

  private void writeEvents(Map<String, Object>... events) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Multimap<String, byte[]> wroteEvents = ArrayListMultimap.create();

    AmazonKinesisAsyncClient client = getStreamToMapClient(wroteEvents);
    CustomerEventIngest ingest = getLambda(mapper, stream, client);
    CustomerEventRequest request = new CustomerEventRequest();
    request.setCustomerKey(orgId);
    request.setEvents(events);
    ingest.handle(request, Mockito.mock(Context.class));

    verifyReadEvents(wroteEvents, mapper, stream, events);
  }

  private void verifyReadEvents(Multimap<String, byte[]> wrote, ObjectMapper mapper, String stream,
    Map<String, Object>... events) throws IOException {
    assertEquals(1, wrote.keySet().size());
    assertEquals(events.length, wrote.get(stream).size());
    Iterator<byte[]> written = wrote.get(stream).iterator();
    for (int i = 0; i < events.length; i++) {
      byte[] eventBytes = written.next();
      Map<String, Object> read =
        mapper.readValue(eventBytes, new TypeReference<Map<String, Object>>() {
        });
      Map<String, Object> event = events[i];
      event.put(AvroSchemaProperties.ORG_ID_KEY, orgId);
      assertEquals(event, read);
    }
  }

  private CustomerEventIngest getLambda(ObjectMapper mapper, String streamName,
    AmazonKinesisAsyncClient client) {
    return new CustomerEventIngest(newArrayList(
      SingleInstanceModule.instanceModule(mapper),
      new SingleInstanceModule<>(client, AmazonKinesisAsyncClient.class),
      namedInstance(CustomerEventHandler.CUSTOMER_EVENT_HANDLER_KINESIS_STREAM, stream)
    ));
  }

  private AmazonKinesisAsyncClient getStreamToMapClient(Multimap<String, byte[]> events) {
    AmazonKinesisAsyncClient client = Mockito.mock(AmazonKinesisAsyncClient.class);
    Mockito.when(client.putRecord(anyString(), any(), anyString())).thenAnswer(invoke -> {
      ByteBuffer buff = (ByteBuffer) invoke.getArguments()[1];
      events.put((String) invoke.getArguments()[0], read(buff));
      return new PutRecordResult();
    });
    Mockito.when(client.putRecords(any())).then(invoke -> {
      PutRecordsRequest request = (PutRecordsRequest) invoke.getArguments()[0];
      String stream = request.getStreamName();
      for (PutRecordsRequestEntry r : request.getRecords()) {
        events.put(stream, read(r.getData()));
      }
      return new PutRecordsResult().withFailedRecordCount(0);
    });
    return client;
  }

  private byte[] read(ByteBuffer buff) {
    byte[] data = new byte[buff.remaining()];
    buff.get(data);
    return data;
  }
}
