# Ingest

Pipeline to ingest reads from customers

## Building

```beans-tomcat``` builds to a ```war``` type, which have a maven bug that prevents it from working with ```mvn test```. Instead, you have to run at least up to the ```package``` phase.

### Test Phases

Tests are broken out into four phases:
  * local tests
  * local aws tests
  * integration tests
  * integration tests using real AWS services

By default, only the first three phases run. You can run each phase indepenently with the proper profile switches:
  * -PlocalTests - just run the basic unit tests
  * -PawsLocalTests - just run the local AWS tests
  * -PintTests - just run the integration tests
  * -PawsIntegration - just run the AWS integration tests
