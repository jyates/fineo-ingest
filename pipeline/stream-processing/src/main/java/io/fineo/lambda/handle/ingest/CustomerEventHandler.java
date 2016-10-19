package io.fineo.lambda.handle.ingest;

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.external.ExternalErrorsUtil;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.schema.store.AvroSchemaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;

/**
 * Handle parsing customer events
 */
public class CustomerEventHandler extends ExternalFacingRequestHandler<CustomerEventRequest,
  CustomerEventResponse> {

  private final CustomerEventResponse SINGLE_EVENT_RESPONSE = new CustomerEventResponse();

  private final long MAX_BYTES = 5000000;
  private final int MAX_EVENTS_PER_REQUEST = 500;
  private final long MAX_EVENT_SIZE = 1000000 - 512;

  private static final Logger LOG = LoggerFactory.getLogger(CustomerEventHandler.class);
  public static final String CUSTOMER_EVENT_HANDLER_KINESIS_STREAM =
    "fineo.kinesis.ingest";

  private final ObjectMapper mapper;
  private final String stream;
  private final AmazonKinesisAsyncClient client;

  @Inject
  public CustomerEventHandler(ObjectMapper mapper,
    @Named(CUSTOMER_EVENT_HANDLER_KINESIS_STREAM) String stream, AmazonKinesisAsyncClient client) {
    this.mapper = mapper;
    this.stream = stream;
    this.client = client;
  }

  @Override
  protected CustomerEventResponse handle(CustomerEventRequest event, Context context)
    throws Exception {
    Preconditions.checkNotNull(event.getCustomerKey(), "Missing customer key from API Gateway");

    return event.getEvent() == null ?
           handleEvents(event, context) :
           handleSingleEvent(event, context);
  }

  private CustomerEventResponse handleEvents(CustomerEventRequest request, Context context)
    throws JsonProcessingException {
    CustomerMultiEventResponse response = new CustomerMultiEventResponse();
    if (request.getEvents() == null || request.getEvents().length == 0) {
      return response;
    }
    if (request.getEvents().length > 500) {
      throw ExternalErrorsUtil.get40X(context, 0,
        "Too many events requested! Max allowed: " + MAX_EVENTS_PER_REQUEST);
    }

    List<byte[]> events =
      Stream.of(request.getEvents())
            .map(e -> {
              try {
                byte[] encoded = encode(context, request, e);
                checkEventSize(encoded, context);
                return encoded;
              } catch (JsonProcessingException e1) {
                throw new RuntimeException(e1);
              }
            })
            .collect(Collectors.toList());

    long size = events.stream().mapToLong(e -> e.length).sum();
    if (size > MAX_BYTES) {
      throw ExternalErrorsUtil.get40X(context, 0,
        "Max allowed size of binary encoded request is " + MAX_BYTES + ". Your request totalled: "
        + size);
    }

    List<PutRecordsRequestEntry> entries =
      events.stream()
            .map(bytes -> new PutRecordsRequestEntry()
              .withData(ByteBuffer.wrap(bytes))
              .withPartitionKey(getPartitionKey(request))).collect(Collectors.toList());

    PutRecordsRequest putRequest = new PutRecordsRequest()
      .withRecords(entries)
      .withStreamName(stream);
    PutRecordsResult putResult = client.putRecords(putRequest);
    return populateErrors(putResult, response);
  }

  private CustomerMultiEventResponse populateErrors(PutRecordsResult result,
    CustomerMultiEventResponse response) {
    if (result.getFailedRecordCount() == 0) {
      return response;
    }

    List<EventResult> results =
      result.getRecords().stream()
            .map(r -> new EventResult().setErrorCode(r.getErrorCode())
                                       .setErrorMessage(r.getErrorMessage()))
            .collect(Collectors.toList());
    return response.setResults(results);
  }

  private CustomerEventResponse handleSingleEvent(CustomerEventRequest request, Context context)
    throws JsonProcessingException {
    byte[] data = encode(context, request, request.getEvent());
    checkEventSize(data, context);
    this.client.putRecord(stream, ByteBuffer.wrap(data), getPartitionKey(request));
    return SINGLE_EVENT_RESPONSE;
  }

  private String getPartitionKey(CustomerEventRequest request) {
    return format("%s - %s", request.getCustomerKey(), Instant.now().toEpochMilli());
  }

  private byte[] encode(Context context, CustomerEventRequest event, Map<String, Object> object)
    throws JsonProcessingException {
    Map<String, Object> outEvent = newHashMap(object);
    outEvent.put(AvroSchemaProperties.ORG_ID_KEY, event.getCustomerKey());
    try {
      return mapper.writeValueAsBytes(outEvent);
    } catch (JsonProcessingException e) {
      try {
        throw ExternalErrorsUtil.get40X(context, 0, "Failed to serialize event object!");
      } catch (JsonProcessingException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  private void checkEventSize(byte[] buff, Context context) {
    if (buff.length <= MAX_EVENT_SIZE) {
      return;
    }
    String msg = "Events must be less than " + MAX_EVENT_SIZE + " bytes when "
                 + "serialized to UTF-8 strings. Your event was: " + buff.length + " bytes.";
    try {

      throw ExternalErrorsUtil.get40X(context, 0, msg);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failure while encoding exception. Root error: " + msg);
    }
  }
}
