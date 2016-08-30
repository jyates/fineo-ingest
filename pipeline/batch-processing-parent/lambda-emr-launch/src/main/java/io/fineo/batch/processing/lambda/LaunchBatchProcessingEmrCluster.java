package io.fineo.batch.processing.lambda;

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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.LambdaHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;

/**
 * Deploy an EMR Cluster to handle the batch procesing
 */
public class LaunchBatchProcessingEmrCluster implements LambdaHandler<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(LaunchBatchProcessingEmrCluster.class);

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
  private final String region;
  private final String clusterName;
  private final AmazonElasticMapReduceClient emr;

  @Inject
  public LaunchBatchProcessingEmrCluster(@Named("aws.region") String region,
    @Named("fineo.batch.cluster.jar") String sourceJar,
    @Named("fineo.batch.cluster.main") String mainClass,
    @Named("fineo.batch.cluster.name") String clusterName, AmazonElasticMapReduceClient emr) {
    this.mainClass = mainClass;
    this.region = region;
    this.sourceJar = sourceJar;
    this.clusterName = clusterName;
    this.emr = emr;
  }

  public void handle(Object event) {
    deploy();
  }

  public void deploy() {
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
      .withLogUri("s3://logs.fineo.io/transient-spark-" + clusterName +
                  "-" + UUID.randomUUID() + "-" + Instant.now().toEpochMilli())
      .withServiceRole(SERVICE_ROLE)
      .withJobFlowRole(EXEC_ROLE)
      .withInstances(instances)
      .withSteps(enabledebugging, spark)
      .withApplications(new Application().withName("Spark"));

    emr.withRegion(RegionUtils.getRegion(region));
    LOG.info("Submitting request: " + request);
    RunJobFlowResult result = emr.runJobFlow(request);
    LOG.info("Got result: " + result);
  }
}
