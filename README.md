# Ingest

Pipeline to ingest reads from customers

## Building

```beans-tomcat``` builds to a ```war``` type, which have a maven bug that prevents it from working with ```mvn test```. Instead, you have to run at least up to the ```package``` phase.

### Test Phases

Tests are broken out into five phases:
  * local tests
  * local aws tests
  * integration tests
  * integration tests using real AWS services
  * deployment validation

By default, only the first three phases run. You can run each phase indepenently with the proper profile switches:
  * -PlocalTests - just run the basic unit tests
  * -PawsLocalTests - just run the local AWS tests
  * -PintTests - just run the integration tests
  * -PawsIntegration - just run the AWS integration tests
  * -Pvalidate - just run the deployment validation. Generally, you should not run this by hand, as it requires deploying code with test versions and linking everything up.

Integration tests are those that start with ```IT```, while unit tests start with ``Test```.
