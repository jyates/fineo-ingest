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

import static java.lang.String.format;

/**
 * Deploy an EMR Cluster to handleEvent the batch procesing
 */
public class LaunchBatchProcessingEmrCluster implements LambdaHandler<Map<String, Object>> {

  private static final Logger LOG = LoggerFactory.getLogger(LaunchBatchProcessingEmrCluster.class);

  // IAM roles
  private static final String EXEC_ROLE = // role for actually doing the work on the cluster
    "Fineo-BatchIngest"; // yeah, just the name, not the arn. Not sure why.
  private static final String SERVICE_ROLE = // role that EMR assumes to _manage_ the cluster
    "arn:aws:iam::766732214526:role/emr-batch-ingest";

  // EC2 Settings
  private static final String RELEASE_LABEL = "emr-4.7.2";
  private static final Integer MASTER_INSTANCES = 1;
  private static final String MASTER_INSTANCE_TYPE = "m1.large";
  private static final Integer CORE_INSTANCES = 3;
  private static final String CORE_INSTANCE_TYPE = "m3.xlarge";

  private static final String EC2_KEY_NAME = "transient-batch-processing-emr_US-East-1";
  //"ElasticMapReduce-master"
  private static final String MASTER_SECURITY_GROUP = "sg-a11403d9";
  //"ElasticMapReduce-slave"
  private static final String CORE_INSTANCE_SECURITY_GROUP = "sg-a21403da";

  // Cluster launching properties
  private static final String FINEO_BATCH_LAUNCH_OVERRIDES_KEY = "fineo.batch.overrides";
  // either specify the jar in properties or use the configured s3 bucket/key
  public static final String FINEO_BATCH_CLUSTER_JAR = "fineo.batch.cluster.jar";
  public static final String FINEO_BATCH_CLUSTER_S3_BUCKET = "fineo.batch.cluster.s3.bucket";
  public static final String FINEO_BATCH_CLUSTER_S3_KEY = "fineo.batch.cluster.s3.key";
  // generic cluster properties
  public static final String FINEO_BATCH_CLUSTER_MAIN = "fineo.batch.cluster.main";
  public static final String FINEO_BATCH_CLUSTER_NAME = "fineo.batch.cluster.name";
  public static final String FINEO_BATCH_CLUSTER_TERMINATION_PROTECTED =
    "fineo.batch.cluster.termination_protected";
  public static final String FINEO_BATCH_CLUSTER_AUTO_TERMINATE =
    "fineo.batch.cluster.auto_terminate";
  private static final String SPARK_EXECUTORS = "fineo.batch.spark.executor.num";
  private static final String SPARK_EXECUTOR_MEMORY = "fineo.batch.spark.executor.memory";
  private static final String SPARK_EXECUTOR_CORES = "fineo.batch.spark.executor.cores";

  private final String sourceJar;
  private final String mainClass;
  private final String region;
  private final String clusterName;
  private final AmazonElasticMapReduceClient emr;

  @Inject
  public LaunchBatchProcessingEmrCluster(@Named("aws.region") String region,
    @Named(FINEO_BATCH_CLUSTER_S3_BUCKET) String bucket,
    @Named(FINEO_BATCH_CLUSTER_S3_KEY) String key,
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
    List<StepConfig> steps = new ArrayList<>();
    steps.add(enableDebugging());
    steps.add(sparkProcessingStep(overrides));

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
      .withEmrManagedMasterSecurityGroup(MASTER_SECURITY_GROUP)
      .withEmrManagedSlaveSecurityGroup(CORE_INSTANCE_SECURITY_GROUP)
      .withEc2KeyName(EC2_KEY_NAME)
      // keep-alive (!auto-termination).
      .withKeepJobFlowAliveWhenNoSteps(!getOrDefault(overrides,
        FINEO_BATCH_CLUSTER_AUTO_TERMINATE, true))
      // by default, allow the cluster to terminate. Can be overridden with event properties
      .withTerminationProtected(getOrDefault(overrides,
        FINEO_BATCH_CLUSTER_TERMINATION_PROTECTED, false));


    RunJobFlowRequest request = new RunJobFlowRequest()
      .withName("Transient Spark Cluster - " + clusterName)
      .withReleaseLabel(RELEASE_LABEL)
      .withConfigurations(getHadoopConfig(),
        getSparkConfig(), getSparkDefaults(overrides), getSparkLog4j())
      .withLogUri(format("s3://logs.fineo.io/transient-spark-%s/%s", clusterName, Instant.now()))
      .withServiceRole(SERVICE_ROLE)
      .withJobFlowRole(EXEC_ROLE)
      .withInstances(instances)
      .withSteps(steps)
      .withVisibleToAllUsers(false)
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

  private Configuration getSparkConfig() {
    return new Configuration()
      .withClassification("spark-env")
      .withConfigurations(getEnvironment());
  }

  private Configuration getSparkLog4j() {
    Map<String, String> logging = new HashMap<>();
    logging.put("log4j.rootLogger", "INFO, CONSOLE");
    logging.put("log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender");
    logging.put("log4j.appender.CONSOLE.target", "System.err");
    logging.put("log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout");
    logging.put("log4j.appender.CONSOLE.layout.conversionPattern",
      "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{3}[%L] - %m%n");
    setLogLevel(logging, "io.fineo", "TRACE");
    // DEBUG adds count-approx hooks, which take a lot of time to calculate
//    setLogLevel(logging, "io.fineo.batch.processing.spark.BatchProcessor", "INFO");
    setLogLevel(logging, "io.fineo.schema.store", "INFO");
    setLogLevel(logging, "io.fineo.schema.timestamp.MultiPatternTimestampParser", "INFO");

    return new Configuration()
      .withClassification("spark-log4j")
      .withProperties(logging);
  }

  private void setLogLevel(Map<String, String> logging, String path, String level) {
    logging.put(format("log4j.logger.%s", path), level);
  }

  private Configuration getEnvironment() {
    Map<String, String> java8 = new HashMap<>();
    java8.put("JAVA_HOME", "/usr/lib/jvm/java-1.8.0");
    return new Configuration()
      .withClassification("export")
      .withProperties(java8);
  }

  private Configuration getSparkDefaults(Map<String, Object> overrides) {
    Map<String, String> props = new HashMap<>();
    props.put("spark.executor.memory", getOrDefault(overrides, SPARK_EXECUTOR_MEMORY, "6g"));
    props.put("spark.yarn.executor.memoryOverhead", "1");
    props.put("spark.executor.cores", getOrDefault(overrides, SPARK_EXECUTOR_CORES, "3"));
    props.put("spark.dynamicAllocation.enabled ", "true");
    return new Configuration()
      .withClassification("spark-defaults")
      .withProperties(props);
  }

  private StepConfig sparkProcessingStep(Map<String, Object> overrides) {
    HadoopJarStepConfig stepConf = new HadoopJarStepConfig()
      .withJar("command-runner.jar")
      .withArgs("spark-submit",
        "--deploy-mode", "cluster",
        "--class", mainClass,
        getOrDefault(overrides, FINEO_BATCH_CLUSTER_JAR, sourceJar));
    return new StepConfig()
      .withName("Spark Batch Processing Step")
      .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
      .withHadoopJarStep(stepConf);
  }

  private StepConfig enableDebugging() {
    return new StepConfig()
      .withName("Enable Debugging")
      .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
      .withHadoopJarStep(new HadoopJarStepConfig()
        .withJar("command-runner.jar")
        .withArgs("state-pusher-script"));
  }

  private <T> T getOrDefault(Map<String, Object> map, String key, T defaultValue) {
    Object out = map.get(key);
    if (out == null) {
      out = defaultValue;
    }
    return (T) out;
  }
}
