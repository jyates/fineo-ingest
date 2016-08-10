package io.fineo.lambda.dynamo.avro;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.StateMachineConfig;
import io.fineo.lambda.aws.AwsAsyncRequest;
import io.fineo.lambda.aws.AwsAsyncSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.fineo.lambda.dynamo.avro.DynamoUpdate.FAILED_INITIAL_UPDATE_MESSAGE;

/**
 * Handle the state machine logic and exception handling required to implement the the actual
 * dynamo update. There is some coupling between this and {@link DynamoUpdate} due to the nature
 * of the exceptions that are thrown. We expect there to be a Condtional exception on the initial
 * request if some maps are not present and a service exception if we get to a point where we
 * reach server that has old state and no new map.
 */
public class DynamoMapUpdater<T> {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoMapUpdater.class);
  private final Function<T, UpdateItemRequest> initialRequest;
  private final Function<UpdateItemRequest, UpdateItemRequest> failedRequestGenerator;
  private final BiFunction<UpdateItemRequest, UpdateItemResult, UpdateItemRequest>
    settingMapFieldsRequest;
  private final AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, T> pool;

  private StateMachine<State, Trigger> machine;
  private UpdateItemRequest nextRequest;
  private T obj;
  private String table;

  public DynamoMapUpdater(Function<T, UpdateItemRequest> initialRequest,
    Function<UpdateItemRequest, UpdateItemRequest> failedInitialUpdateGenerator,
    BiFunction<UpdateItemRequest, UpdateItemResult, UpdateItemRequest> settingMapFieldsRequest,
    AwsAsyncSubmitter<UpdateItemRequest, UpdateItemResult, T> pool) {
    this.initialRequest = initialRequest;
    // wrap the request handling to ensure that we only handle the update on the right failure msg
    this.failedRequestGenerator = failedInitialUpdateGenerator;
    this.settingMapFieldsRequest = settingMapFieldsRequest;
    this.pool = pool;
  }

  enum State {
    START,
    INITIAL_REQUEST,
    WAITING_TO_COMPLETE_SETTING_MAPS,
    UPDATING_MAPS,
    DONE;
  }

  enum Trigger {
    START,
    FAILED_INITIAL_REQUEST,
    COMPLETED_INITIAL_REQUEST,
    UPDATED_MAPS,
    RETRY_SETTING_MAPS,
    ALL_FIELDS_SET,
    SET_BASE_MAPS_AWAITING_UPDATES;
  }


  public void submit(T object, String table) {
    this.table = table;
    this.obj = object;
    UpdateItemRequest request = initialRequest.apply(object);
    request.setTableName(table);

    StateMachineConfig<State, Trigger> sm = new StateMachineConfig<>();
    sm.configure(State.INITIAL_REQUEST)
      .onEntry(() -> this.submitInitialRequest(request))
      .permitReentry(Trigger.START)
      .permit(Trigger.FAILED_INITIAL_REQUEST, State.WAITING_TO_COMPLETE_SETTING_MAPS)
      .permit(Trigger.COMPLETED_INITIAL_REQUEST, State.DONE);

    sm.configure(State.WAITING_TO_COMPLETE_SETTING_MAPS)
      .onEntry(this::handleFailedMap)
      .permit(Trigger.ALL_FIELDS_SET, State.DONE)
      .permit(Trigger.SET_BASE_MAPS_AWAITING_UPDATES, State.UPDATING_MAPS);

    sm.configure(State.UPDATING_MAPS)
      .onEntry(this::updateMaps)
      .permit(Trigger.RETRY_SETTING_MAPS, State.WAITING_TO_COMPLETE_SETTING_MAPS)
      .permit(Trigger.UPDATED_MAPS, State.DONE);

    sm.configure(State.DONE)
      .onEntry(() -> {
        LOG.info("Dynamo update completed!");
      });

    this.machine = new StateMachine<>(State.INITIAL_REQUEST, sm);
    this.machine.fire(Trigger.START);
  }

  public void submitInitialRequest(UpdateItemRequest ui) {
    this.nextRequest = ui;
    pool.submit(onFailureChecker(e -> e instanceof ConditionalCheckFailedException, Trigger
      .FAILED_INITIAL_REQUEST, Trigger.COMPLETED_INITIAL_REQUEST));
  }

  private UpdateItemRequest setNextRequest(UpdateItemRequest request) {
    if (request != null) {
      request.setTableName(table);
    }
    this.nextRequest = request;
    return request;
  }

  /**
   * Called from {@link State#WAITING_TO_COMPLETE_SETTING_MAPS}
   */
  public void handleFailedMap() {
    AwsAsyncRequest<T, UpdateItemRequest> request =
      new AwsAsyncRequest<T, UpdateItemRequest>(this.obj, this.nextRequest) {
        @Override
        public <RESULT> void onSuccess(UpdateItemRequest request, RESULT result) {
          if (setNextRequest(
            settingMapFieldsRequest.apply(request, (UpdateItemResult) result)) == null) {
            machine.fire(Trigger.ALL_FIELDS_SET);
          } else {
            machine.fire(Trigger.SET_BASE_MAPS_AWAITING_UPDATES);
          }
        }
      };
    pool.submit(request);
  }

  /**
   * Called from {@link State#UPDATING_MAPS}
   */
  public void updateMaps() {
    pool.submit(onFailureChecker(
      exception -> exception instanceof AmazonServiceException &&
                   exception.getMessage().contains(FAILED_INITIAL_UPDATE_MESSAGE),
      Trigger.RETRY_SETTING_MAPS, Trigger.UPDATED_MAPS));
  }

  /**
   * Helper to generate a request that generates an setting value when the exception passes the
   * specified predicate, or just retries otherwise. On success, the 'success' trigger is triggered.
   *
   * @return request to run in the pool
   */
  private AwsAsyncRequest onFailureChecker(Predicate<Exception> triggerException, Trigger
    triggerOnExceptionPass, Trigger success) {
    UpdateItemRequest initialRequest = this.nextRequest;
    return new AwsAsyncRequest<T, UpdateItemRequest>(this.obj, this.nextRequest) {
      @Override
      public boolean onError(Exception exception) {
        if (triggerException.test(exception)) {
          // generate the nextRequest for the next state
          setNextRequest(failedRequestGenerator.apply(initialRequest));
          DynamoMapUpdater.this.nextRequest.setTableName(table);
          machine.fire(triggerOnExceptionPass);
          return false;
        }
        return super.onError(exception);
      }

      @Override
      public <RESULT> void onSuccess(UpdateItemRequest request, RESULT result) {
        machine.fire(success);
      }
    };
  }
}
