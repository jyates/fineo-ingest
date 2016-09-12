package io.fineo.lambda.handle.ingest;

import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.FlushResponse;
import io.fineo.lambda.aws.MultiWriteFailures;
import io.fineo.lambda.handle.external.ExternalErrorsUtil;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.kinesis.SingleStreamKinesisProducer;
import io.fineo.schema.Pair;
import io.fineo.schema.store.AvroSchemaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Base64;
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
  private static final Logger LOG = LoggerFactory.getLogger(CustomerEventHandler.class);

  private static final Base64.Encoder ENCODER = Base64.getEncoder();
  private final ObjectMapper mapper;
  private final SingleStreamKinesisProducer producer;

  @Inject
  public CustomerEventHandler(ObjectMapper mapper, SingleStreamKinesisProducer producer) {
    this.mapper = mapper;
    this.producer = producer;
  }

  @Override
  protected CustomerEventResponse handle(CustomerEventRequest event, Context context)
    throws Exception {
    if (event.getCustomerKey() == null) {
      throw ExternalErrorsUtil.get500(context, "Missing customer key from API Gateway");
    }

    return event.getEvent() == null || event.getEvent().size() == 0 ?
           handleEvents(event, context) :
           handleSingleEvent(event, context);
  }

  private CustomerEventResponse handleEvents(CustomerEventRequest request, Context context)
    throws JsonProcessingException {
    CustomerMultiEventResponse response = new CustomerMultiEventResponse();
    if (request.getEvents() == null || request.getEvents().length == 0) {
      return response;
    }
    List<Pair<byte[], Object>> events =
      Stream.of(request.getEvents())
            .map(e -> new Pair<byte[], Object>(encode(context, request, e), e))
            .collect(Collectors.toList());
    producer.write(getPartitionKey(request), events);
    FlushResponse<List<Object>, PutRecordsRequest> out = producer.flushEvents();
    MultiWriteFailures<List<Object>, PutRecordsRequest> failures = out.getFailures();
    if (failures.any()) {
      assert failures.getActions().size() == 1 :
        "More than one failure result when sending a batch of records!";
      AwsAsyncRequest<List<Object>, PutRecordsRequest> failed = failures.getActions().get(0);
      Exception cause = failed.getException();
      throw ExternalErrorsUtil.get500(context, format("Kinesis error: %s - %s \n%s", cause
        .getClass(), cause.getMessage() + Joiner.on("\n(").join(cause.getStackTrace())));
    }

    assert out.getCompleted().size() == 1 : "More than on success when sending a batch of records";
    AwsAsyncRequest<List<Object>, PutRecordsRequest> success = out.getCompleted().get(0);
    PutRecordsResult result = success.getResult();
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
    data = ENCODER.encode(data);
    producer.write(getPartitionKey(request), data, request.getEvent());
    MultiWriteFailures<Object, PutRecordRequest> response =
      producer.flushSingleEvent().getFailures();
    // figure out which records didn't get sent
    if (response.any()) {
      AwsAsyncRequest<Map<String, Object>, PutRecordRequest> actions =
        (AwsAsyncRequest<Map<String, Object>, PutRecordRequest>) response.getActions();
      Exception error = actions.getException();
      String base = "Unexpected exception: " + error.getClass() + ": ";
      if (error instanceof InvalidArgumentException || error instanceof ResourceNotFoundException) {
        base = "Malformed request to Kinesis: ";
      } else if (error instanceof ProvisionedThroughputExceededException) {
        LOG.error("Hit kinesis throughput limits!", error);
        base = "Internal throughput error - Kinesis:";
      }
      throw ExternalErrorsUtil
        .get500(context, base + error.getMessage());
    }
    return new CustomerEventResponse();
  }

  private String getPartitionKey(CustomerEventRequest request) {
    return format("%s - %s", request.getCustomerKey(), Instant.now().toEpochMilli());
  }

  private byte[] encode(Context context, CustomerEventRequest event, Map<String, Object> object) {
    Map<String, Object> outEvent = newHashMap(event.getEvent());
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
}
