package io.fineo.lambda.aws;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public class AwsAsyncSubmitter<S extends AmazonWebServiceRequest, R, B> {

  private static final Log LOG = LogFactory.getLog(AwsAsyncSubmitter.class);

  private final long retries;
  private final Submitter<S, R> client;

  private volatile Phaser phase = newPhaser();
  private final List<UpdateItemHandler> actions = Collections.synchronizedList(new ArrayList<>());
  private final List<AwsAsyncRequest<B, S>> failed =
    Collections.synchronizedList(new ArrayList<>(0));

  public AwsAsyncSubmitter(long retries, Submitter<S, R> client) {
    if (retries < 0) {
      retries = 1;
    }
    this.retries = retries;
    this.client = client;
  }

  public void submit(AwsAsyncRequest<B, S> request) {
    Phaser phase = this.phase;
    UpdateItemHandler handler = new UpdateItemHandler(phase, request);
    actions.add(handler);
    register(phase, "Submitting request: " + request);
    submit(handler);
  }

  private void register(Phaser phase, String msg) {
    int p = phase.register();
    LOG.trace("REGISTER(" + p + ") - " + msg);
  }

  private void submit(UpdateItemHandler handler) {
    if (handler.attempts >= retries) {
      this.actions.remove(handler);
      this.failed.add(handler.request);
      done(handler.phaser, "Actions exceeded retries");
      return;
    }
    LOG.trace("Resubmitting!");
    client.submit(handler.getRequest(), handler);
  }

  /**
   * Flush all writes that were submitted before calling {@link #flush()}. Some writes may still
   * be outstanding after flush, if they were submitted on a different thread to <tt>thius</tt>.
   *
   * @return any failures that occurred
   */
  public MultiWriteFailures<B> flush() {
    Phaser phaser = this.phase;
    this.phase = newPhaser();

    register(phaser, "Flushing");
    phaser.awaitAdvance(done(phaser, "Flushing - waiting advance"));
    LOG.trace("Flushed completed => Advanced"); ;
    return new MultiWriteFailures(failed);
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
    LOG.trace("DE-REGISTER(" + p + "): " + msg);
    return p;
  }

  public class UpdateItemHandler implements AsyncHandler<S, R> {

    private final Phaser phaser;
    private int attempts = 0;

    private final AwsAsyncRequest<B, S> request;

    public UpdateItemHandler(Phaser phase, AwsAsyncRequest<B, S> request) {
      this.phaser = phase;
      this.request = request;
    }

    public S getRequest() {
      return request.getRequest();
    }

    @Override
    public void onError(Exception exception) {
      LOG.error("Failed to make an update for request: " + this.request, exception);
      attempts++;
      submit(this);
    }

    @Override
    public void onSuccess(S request, R updateItemResult) {
      // remove the request from the pending list because we were successful
      LOG.debug("Update success: " + this);
      actions.remove(this);
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
