# Ingest

Pipeline to ingest reads from customers

### Test Phases

Tests are broken out into five phases, of which only the first three run by default

  * local tests
  * local aws tests
  * integration tests
  --- require special triggers ---
  * integration tests using real AWS services
  * deployment validation

You can run each phase independently with the proper profile switches:

  * -D localTests
    * just run the basic unit tests
  * -D awsLocalTests
    * just run the local AWS tests
  * -D intTests
    * just run the integration tests
  * -D awsIntegration
    * just run the AWS integration tests
  * -D validate
    * just run the deployment validation. Generally, you should not run this by hand, as it requires deploying code with test versions and linking everything up.

Integration tests are those that start with ```IT```, while unit tests start with ``Test```.

### Drill/Spark Integration

Currently, we don't have support to run Drill and Spark in the same JVM. This precludes easily testing them together for the read/write paths. However, by specifying:

```-Dfineo.spark.dir=<somedir>```

when you run the tests, then Spark will write some output data to that directory while Drill will attempt to read records from that directory.
