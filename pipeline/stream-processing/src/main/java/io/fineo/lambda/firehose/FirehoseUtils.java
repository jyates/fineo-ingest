package io.fineo.lambda.firehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirehoseUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FirehoseUtils.class);
  private static final long FIREHOSE_CREATING_WAIT_MS = 500;

  private FirehoseUtils() {
    // private ctor for utils
  }

  public static void checkHoseStatus(AmazonKinesisFirehoseClient firehoseClient, String
    deliveryStreamName) {
    DescribeDeliveryStreamRequest describeHoseRequest = new DescribeDeliveryStreamRequest()
      .withDeliveryStreamName(deliveryStreamName);
    DescribeDeliveryStreamResult describeHoseResult;
    String status = "";
    try {
      describeHoseResult = firehoseClient.describeDeliveryStream(describeHoseRequest);
      status = describeHoseResult.getDeliveryStreamDescription().getDeliveryStreamStatus();
    } catch (Exception e) {
      LOG.error("Firehose " + deliveryStreamName + " Not Existent", e);
      throw new RuntimeException(e);
    }
    if (status.equalsIgnoreCase("ACTIVE")) {
      LOG.debug("Firehose ACTIVE " + deliveryStreamName);
      //return;
    } else if (status.equalsIgnoreCase("CREATING")) {
      LOG.debug("Firehose CREATING " + deliveryStreamName);
      try {
        Thread.sleep(FIREHOSE_CREATING_WAIT_MS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      checkHoseStatus(firehoseClient, deliveryStreamName);
    } else {
      LOG.debug("Status = " + status);
    }
  }
}
