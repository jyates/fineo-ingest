# Ingest

Pipeline to ingest reads from customers

## Building

```beans-tomcat``` builds to a ```war``` type, which have a maven bug that prevents it from working with ```mvn test```. Instead, you have to run at least up to the ```package``` phase.

### Test Phases

Tests are broken out into five phases, of which only the first three run

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
