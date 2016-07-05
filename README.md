# Ingest

Pipeline to ingest reads from customers

## Modules

|Name|Desc|
|---|---|
|common|Common properties and utilities across packages|
|dynamo-common|Common dynamo utilities|
|lambda-common|Utilities for lambda functions|
|pipeline-utils|Testing utilities for the ingest pipeline|
|spark-common|Common spark utilities across spark jobs|
|----|---|
|drill-test|Run the drill read tests against the spark output files|
|lambda-validate|Test rig for validating end-to-end lambda functions |
|----|---|
|pipeline|Parent for the different pars of the pipeline|
|batch-etl|ETL from S3 stream output files to S3 parquet/JSON files|
|batch-processing-parent|Parent for the batch version of the pipeline|
|lambda-emr-launch|Launching an EMR cluster for batch processing from lambda|
|lambda-prepare|Lambda handlers to prepare for a batch ingest, i.e. setup batch manifest|
|dynamo-manifest|Manage the S3 based manifest for batch processing|
|sns-handler|Lambda handler for SNS messages for batch processing from S3/external s3 sources|
|dynamo-access|Managing access to dynamo tables|
|lambda-chores|Time-based (e.g. chron) lambda chores|
|stream-processing|Lambda functions to do the ingest processing in a 'streaming' style|

## Testing

Tests are broken out into five phases, of which only the first three run by default:

  * local tests
  * local aws tests
  * integration tests

 --- Requires profile switches ---
  
  * integration tests using real AWS services
  * deployment validation

You can run each phase independently with the proper profile switches:

  * -DlocalTests
    * just run the basic unit tests
  * -DawsLocalTests
    * just run the local AWS tests
  * -DintTests
    * just run the integration tests
  * -DawsIntegration
    * just run the AWS integration tests
  * -Dvalidate
    * just run the deployment validation. Generally, you should not run this by hand, as it requires deploying code with test versions and linking everything up.

Integration tests are those that start with ```IT```, while unit tests start with ```Test```.

### Drill/Spark Integration

Currently, we don't have support to run Drill and Spark in the same JVM. This precludes easily testing them together for the read/write paths. However, by specifying:

```
-Dfineo.spark.dir=<somedir>
```

when you run the tests, then Spark will write some output data to that directory while Drill will attempt to read records from that directory.
