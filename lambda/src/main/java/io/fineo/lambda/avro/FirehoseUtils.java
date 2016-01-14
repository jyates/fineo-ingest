package io.fineo.lambda.avro;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
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
    AmazonKinesisFirehoseAsyncClient firehoseClient = createFireHose(props);
    for (String name : names) {
      checkHoseStatus(firehoseClient, name);
    }

    return firehoseClient;
  }

  public static AmazonKinesisFirehoseAsyncClient createFireHose(LambdaClientProperties props) {
    AmazonKinesisFirehoseAsyncClient firehoseClient = new AmazonKinesisFirehoseAsyncClient();
    firehoseClient.setEndpoint(props.getFirehoseUrl());
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