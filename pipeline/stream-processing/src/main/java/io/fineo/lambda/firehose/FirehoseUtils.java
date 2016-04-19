package io.fineo.lambda.firehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import io.fineo.lambda.configure.legacy.LambdaClientProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FirehoseUtils {

  private static final Log LOG = LogFactory.getLog(FirehoseUtils.class);
  private static final long FIREHOSE_CREATING_WAIT_MS = 500;

  private FirehoseUtils() {
    // private ctor for utils
  }

  public static AmazonKinesisFirehoseAsyncClient createFirehoseAndCheck(
    LambdaClientProperties props, String... names) {
    AmazonKinesisFirehoseAsyncClient firehoseClient = props.createFireHose();
    for (String name : names) {
      checkHoseStatus(firehoseClient, name);
    }

    return firehoseClient;
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
