package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Phaser;

/**
 * Submit async requests to Amazon services. Manages complexity around retries and failed actions
 * in an internally thread-safe manner. All actions to the service can be sent via
 * {@link #submit(AwsAsyncRequest)}. And will be completed asynchonrously. You can wait on all
 * the submitted events to complete in a call to the blocking method {@link #flush()}.
 * <p>
 * {@link #flush()} will provide any failures (actions that failed after the given number of
 * retries were attempted) so you can handle sending the failed records somewhere.
 * {@link AwsAsyncRequest} is designed to allow you to retrieve the original record, before being
 * turned into an {@link AmazonWebServiceRequest}, so you can easily handle the failure.
 * </p>
 * <p>
 * NOTE: this class is not <b>externally threadsafe</b>. If you call {@link #flush()} in one
 * thread and {@link #submit(AwsAsyncRequest)} in another thread, it is not safe to assume that
 * the {@link #flush()} will block for the potentially submitted request. Generally, you should
 * use this class is a single threaded manner, leveraging the async capabilities to send a bunch
 * of requests, and then wait on all those requests to complete via {@link #flush()}.
 * </p>
 */
public class AwsAsyncSubmitter<REQUEST extends AmazonWebServiceRequest, RESPONSE extends
  Serializable, BASE_REQUEST> {

  private static final Logger LOG = LoggerFactory.getLogger(AwsAsyncSubmitter.class);

  private final long retries;
  private final Submitter<REQUEST, RESPONSE> client;

  private volatile Phaser phase = newPhaser();
  private final List<UpdateItemHandler> actions = Collections.synchronizedList(new ArrayList<>());
  private List<AwsAsyncRequest<BASE_REQUEST, REQUEST>> completed = new ArrayList<>();
  private final List<AwsAsyncRequest<BASE_REQUEST, REQUEST>> failed =
    Collections.synchronizedList(new ArrayList<>(0));

  public AwsAsyncSubmitter(long retries, Submitter<REQUEST, RESPONSE> client) {
    if (retries < 0) {
      retries = 1;
    }
    this.retries = retries;
    this.client = client;
  }

  public void submit(AwsAsyncRequest<BASE_REQUEST, REQUEST> request) {
    Phaser phase = this.phase;
    UpdateItemHandler handler = new UpdateItemHandler(phase, request);
    actions.add(handler);
    register(phase, "Submitting request: " + request);
    submit(handler);
  }

  private void register(Phaser phase, String msg) {
    int p = phase.register();
    LOG.trace("REGISTER({}[p:{}])) - {}", p, phase.getUnarrivedParties(), msg);
  }

  private void submit(UpdateItemHandler handler) {
    if (handler.attempts >= retries) {
      fail(handler, "Actions exceeded retries");
      return;
    }
    LOG.trace("Resubmitting!");
    client.submit(handler.getRequest(), handler);
  }

  private void fail(UpdateItemHandler handler, String msg) {
    this.actions.remove(handler);
    this.failed.add(handler.request);
    done(handler.phaser, msg);
  }

  /**
   * Flush all writes that were submitted before calling {@link #flush()}. Waits for all elements
   * in the phaser to complete. Some things may be submitted to the phaser after we call flush -
   * we will wait for those too. There is a slight
   *
   * @return any failures that occurred
   */
  public MultiWriteFailures<BASE_REQUEST, REQUEST> flush() {
    return flushRequests().getFailures();
  }

  public FlushResponse<BASE_REQUEST, REQUEST> flushRequests() {
    Phaser phaser = this.phase;
    register(phaser, "Flushing");
    List<AwsAsyncRequest<BASE_REQUEST, REQUEST>> completed = this.completed;
    phaser.awaitAdvance(done(phaser, "Flushing - waiting advance"));
    this.completed = new ArrayList<>();
    LOG.trace("Flushed completed => Advanced"); ;
    MultiWriteFailures failures = new MultiWriteFailures(failed);
    return new FlushResponse<>(completed, failures);
  }

  private static Phaser newPhaser() {
    return new Phaser() {
      protected boolean onAdvance(int phase, int parties) {
        return false;
      }
    };
  }

  private int done(Phaser phaser, String msg) {
    int p = phaser.arriveAndDeregister();
    LOG.trace("DE-REGISTER({}[p:{}]): {}", p, phaser.getUnarrivedParties(), msg);
    return p;
  }

  public class UpdateItemHandler implements AsyncHandler<REQUEST, RESPONSE> {

    private final Phaser phaser;
    private int attempts = 0;

    private final AwsAsyncRequest<BASE_REQUEST, REQUEST> request;

    public UpdateItemHandler(Phaser phase, AwsAsyncRequest<BASE_REQUEST, REQUEST> request) {
      this.phaser = phase;
      this.request = request;
    }

    public REQUEST getRequest() {
      return request.getRequest();
    }

    @Override
    public void onError(Exception exception) {
      LOG
        .trace("{}: Failed to make an update for request: {}", exception.getMessage(), this.request,
          exception);
      attempts++;
      try {
        if (request.onError(exception)) {
          submit(this);
        } else {
          done(this.phaser, "Had error, but marked completed: " + this);
        }
      } catch (Exception e) {
        String msg = "Fatal exception when processing error!";
        LOG.error(msg, e);
        // make sure we don't attempt this again
        this.attempts = Integer.MAX_VALUE;
        fail(this, msg);
      }
    }

    @Override
    public void onSuccess(REQUEST request, RESPONSE updateItemResult) {
      // remove the request from the pending list because we were successful
      LOG.debug("Update success: " + this);
      this.request.onSuccess(request, updateItemResult);
      actions.remove(this);
      completed.add(this.request);
      done(this.phaser, "Completed update: " + this);
    }

    @Override
    public String toString() {
      return "UpdateItemHandler{" +
             "attempts=" + attempts +
             ", request=" + request +
             '}';
    }
  }
}
