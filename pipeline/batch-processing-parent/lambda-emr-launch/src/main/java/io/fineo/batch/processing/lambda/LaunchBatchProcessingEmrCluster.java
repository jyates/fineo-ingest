package io.fineo.batch.processing.lambda;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;

/**
 * Deploy an EMR Cluster to handleEvent the batch procesing
 */
public class LaunchBatchProcessingEmrCluster implements LambdaHandler<Map<String, Object>> {

  private static final Logger LOG = LoggerFactory.getLogger(LaunchBatchProcessingEmrCluster.class);

  // IAM roles
  private static final String EXEC_ROLE = // role for actually doing the work on the cluster
    "transient-spark-emr-exec-role"; // yeah, just the name, not the arn. Not sure why.
  private static final String SERVICE_ROLE = // role that EMR assumes to _manage_ the cluster
    "arn:aws:iam::766732214526:role/transient-spark-emr-service-role";

  // EC2 Settings
  private static final String RELEASE_LABEL = "emr-4.7.2";
  private static final String MASTER_INSTANCE_TYPE = "m1.large";
  private static final String CORE_INSTANCE_TYPE = "m3.xlarge";
  private static final Integer CORE_INSTANCES = 2;
  private static final Integer MASTER_INSTANCES = 1;
  private static final String EC2_KEY_NAME = "transient-batch-processing-emr_US-East-1";
  private static final String MASTER_SECURITY_GROUP = "sg-a11403d9";//"ElasticMapReduce-master"
  private static final String CORE_INSTANCE_SECURITY_GROUP = "sg-a21403da";//"ElasticMapReduce-slave"

  // Cluster launching properties
  private static final String FINEO_BATCH_LAUNCH_OVERRIDES_KEY = "fineo.batch.overrides";
  public static final String FINEO_BATCH_CLUSTER_JAR = "fineo.batch.cluster.jar";
  public static final String FINEO_BATCH_CLUSTER_MAIN = "fineo.batch.cluster.main";
  public static final String FINEO_BATCH_CLUSTER_NAME = "fineo.batch.cluster.name";

  private final String sourceJar;
  private final String mainClass;
  private final String region;
  private final String clusterName;
  private final AmazonElasticMapReduceClient emr;

  @Inject
  public LaunchBatchProcessingEmrCluster(@Named("aws.region") String region,
    @Named("fineo.batch.cluster.s3.bucket") String bucket,
    @Named("fineo.batch.cluster.s3.key") String key,
    @Named(FINEO_BATCH_CLUSTER_MAIN) String mainClass,
    @Named(FINEO_BATCH_CLUSTER_NAME) String clusterName,
    AmazonElasticMapReduceClient emr) {
    this.mainClass = mainClass;
    this.region = region;
    this.sourceJar = format("s3://%s/%s", bucket, key);
    this.clusterName = clusterName;
    this.emr = emr;
  }

  public void handle(Map<String, Object> event) {
    Map<String, Object> overrides =
      (Map<String, Object>) event.get(FINEO_BATCH_LAUNCH_OVERRIDES_KEY);
    if (overrides == null) {
      overrides = new HashMap<>(0);
    }

    StepFactory factory = new StepFactory(region + ".elasticmapreduce");
    List<StepConfig> steps = new ArrayList<>();
    steps.add(enableDebugging(factory));

    steps.add(sparkProcessingStep(factory, overrides));

    InstanceGroupConfig master = new InstanceGroupConfig()
      .withInstanceRole(InstanceRoleType.MASTER)
      .withInstanceType(MASTER_INSTANCE_TYPE)
      .withInstanceCount(MASTER_INSTANCES);
    InstanceGroupConfig core = new InstanceGroupConfig()
      .withInstanceRole(InstanceRoleType.CORE)
      .withInstanceType(CORE_INSTANCE_TYPE)
      .withInstanceCount(CORE_INSTANCES);

    JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
      .withInstanceGroups(master, core)
      .withKeepJobFlowAliveWhenNoSteps(true)
      .withTerminationProtected(true)
      .withEmrManagedMasterSecurityGroup(MASTER_SECURITY_GROUP)
      .withEmrManagedSlaveSecurityGroup(CORE_INSTANCE_SECURITY_GROUP)
      .withEc2KeyName(EC2_KEY_NAME);

    RunJobFlowRequest request = new RunJobFlowRequest()
      .withName("Transient Spark Cluster - " + clusterName)
      .withReleaseLabel(RELEASE_LABEL)
      .withConfigurations(getHadoopConfig(), getSparkConfig())
      .withLogUri("s3://logs.fineo.io/transient-spark-" + clusterName + "/" + Instant.now())
      .withServiceRole(SERVICE_ROLE)
      .withJobFlowRole(EXEC_ROLE)
      .withInstances(instances)
      .withSteps(steps)
      .withApplications(new Application().withName("Spark"));

    emr.withRegion(RegionUtils.getRegion(region));
    LOG.info("Submitting request: " + request);
    RunJobFlowResult result = emr.runJobFlow(request);
    LOG.info("Got result: " + result);
  }

  private Configuration getHadoopConfig() {
    return new Configuration()
      .withClassification("hadoop-env")
      .withConfigurations(getEnvironment());
  }

  private Configuration getSparkConfig(){
    return new Configuration()
      .withClassification("spark-env")
      .withConfigurations(getEnvironment());
  }

  private Configuration getEnvironment(){
    Map<String, String> java8 = new HashMap<>();
    java8.put("JAVA_HOME", "/usr/lib/jvm/java-1.8.0");
    return new Configuration()
      .withClassification("export")
      .withProperties(java8);
  }

  private StepConfig sparkProcessingStep(StepFactory factory, Map<String, Object> overrides) {
    return new StepConfig()
      .withName("Spark Step")
      .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
      .withHadoopJarStep(factory.newScriptRunnerStep("/home/hadoop/spark/bin/spark-submit",
        "--deploy-mode", "cluster",
        "--class", mainClass,
        getOrDefault(overrides, FINEO_BATCH_CLUSTER_JAR, sourceJar)));
  }

  private StepConfig enableDebugging(StepFactory factory) {
    return new StepConfig()
      .withName("Enable Debugging")
      .withActionOnFailure("TERMINATE_JOB_FLOW")
      .withHadoopJarStep(new HadoopJarStepConfig()
        .withJar("command-runner.jar")
        .withArgs("state-pusher-script"));
//    return new StepConfig()
//      .withName("Enable debugging")
//      .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
//      .withHadoopJarStep(factory.newEnableDebuggingStep());
  }

  private <T> T getOrDefault(Map<String, Object> map, String key, T defaultValue) {
    Object out = map.get(key);
    if (out == null) {
      out = defaultValue;
    }
    return (T) out;
  }
}
