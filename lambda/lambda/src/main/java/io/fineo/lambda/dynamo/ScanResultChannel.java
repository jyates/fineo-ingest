package io.fineo.lambda.dynamo;

import com.amazonaws.services.dynamodbv2.document.Attribute;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class ScanResultChannel<ITEM> implements Channel {

  private LinkedBlockingQueue<ITEM> resultQueue = new LinkedBlockingQueue<>();
  private AtomicBoolean open = new AtomicBoolean(true);

  public void offer(ITEM item){
    resultQueue.offer(item);
  }

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public void close() {
    this.open.set(false);
  }

  public ITEM next() throws ClosedChannelException {
    // wait on the next result
    while (this.isOpen()) {
      try {
        return resultQueue.take();
      } catch (InterruptedException e) {
        // only done unless not open any more
      }
    }
    throw new ClosedChannelException();
  }

  /**
   * Get all the remaining items in the channel. Used when the channel is closed, but you want to
   * get the data that was passed before it was closed
   * @return all the remaining items in the channel
   */
  public Queue<ITEM> drain(){
    return resultQueue;
  }
}
