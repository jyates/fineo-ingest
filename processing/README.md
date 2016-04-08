# Lamba Functions

Implementations of lambda functions for fineo.

## Building

Basic testing requirements:
 * Maven 3.X
 * Java 8

Additional deployment requirements
 * RVM - Ruby Version Manager

## Deploying


### 1. Setup Environment (first time only)

```$ setup-deploy.sh```

This installs the correct ruby dependencies

### 2. Build Deployable Jar

```$ deploy/build.rb```

It's a light wrapper around maven that adds properties file we can use to set connection information


### 3. Deploy jar to aws

```$ deploy/run.rb```

### Deployment Validation

You can deploy a test version with the simple command:

```$ test.sh```

This will build a deployable lambda jar with test versions, deploy it to AWS and then run a validation test against the deployment and then cleanup all the test resources (kinesis pipes, lambda event sources, firehoses, dynamo tables, etc.) used for the validation.

However, the lambda function is **not deleted**, so you will have to publish a new version with a new configuration (see above).  

## Testing

Most testing should be done from the parent ```ingest-parent``` directory to enable you to use the simpler test selection parameters. See the [parent pom](../pom.xml) for the properties you need to set to enable more extensive tests.
