package io.fineo.etl.deploy;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.time.Instant;
import java.util.UUID;

/**
 * Deploy an EMR Cluster
 */
public class LaunchSparkEmrCluster {

  private static final Log LOG = LogFactory.getLog(LaunchSparkEmrCluster.class);

  public static final String SOURCE_JAR_LOCATION_KEY = "fineo.deploy.spark.jar-location";

  // IAM roles
  private static final String EXEC_ROLE =
    "arn:aws:iam::766732214526:role/transient-spark-emr-exec-role";
  private static final String SERVICE_ROLE =
    "arn:aws:iam::766732214526:role/transient-spark-emr-service-role";

  // EC2 Settings
  private static final String MASTER_INSTANCE_TYPE = "m1.large";
  private static final String CORE_INSTANCE_TYPE = "m4.large";
  private static final String RELEASE_LABEL = "emr-4.6.0";
  private static final Integer CORE_INSTANCES = 2;
  private static final String EC2_KEY_NAME = "transient-batch-processing-emr";

  private final String sourceJar;
  private final String mainClass;
  private final AWSCredentialsProvider credentials;
  private final String region;
  private final String clusterName;

  public LaunchSparkEmrCluster(String sourceJar, String mainClass,
    AWSCredentialsProvider credentials, String region,
    String clusterName) {
    this.mainClass = mainClass;
    this.region = region;
    this.sourceJar = sourceJar;
    this.credentials = credentials;
    this.clusterName = clusterName;
  }

  public void deploy() {
    AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);
    StepFactory factory = new StepFactory(region + ".elasticmapreduce");

    StepConfig enabledebugging = new StepConfig()
      .withName("Enable debugging")
      .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
      .withHadoopJarStep(factory.newEnableDebuggingStep());

    StepConfig spark = new StepConfig()
      .withName("Spark Step")
      .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
      .withHadoopJarStep(factory.newScriptRunnerStep("/home/hadoop/spark/bin/spark-submit",
        "--deploy-mode", "cluster",
        "--class", mainClass,
        sourceJar));

    InstanceGroupConfig master = new InstanceGroupConfig()
      .withInstanceRole(InstanceRoleType.MASTER)
      .withInstanceType(MASTER_INSTANCE_TYPE);
    InstanceGroupConfig core = new InstanceGroupConfig()
      .withInstanceRole(InstanceRoleType.CORE)
      .withInstanceType(CORE_INSTANCE_TYPE)
      .withInstanceCount(CORE_INSTANCES);

    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
      .withInstanceGroups(master, core)
      .withKeepJobFlowAliveWhenNoSteps(true)
      .withEc2KeyName(EC2_KEY_NAME);

    RunJobFlowRequest request = new RunJobFlowRequest()
      .withName("Transient Spark Cluster - " + clusterName)
      .withReleaseLabel(RELEASE_LABEL)
      .withLogUri("s3://fineo.io/transient-spark-" + clusterName +
                  "-" + UUID.randomUUID() + "-" + Instant.now().toEpochMilli())
      .withServiceRole(SERVICE_ROLE)
      .withJobFlowRole(EXEC_ROLE)
      .withInstances(instances)
      .withSteps(enabledebugging, spark)
      .withApplications(new Application().withName("Spark"));

    emr.withRegion(RegionUtils.getRegion(region));
    LOG.info("Submitting request: "+request);
    RunJobFlowResult result = emr.runJobFlow(request);
    LOG.info("Got result: "+result);
  }
}
