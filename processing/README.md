# Lamba Functions

Implementations of lambda functions for fineo.

## Building

Basic testing requirements:
 * Maven 3.X
 * Java 8

Additidonal deployment requirements
 * RVM - Ruby Version Manager

### Testing
Basic tests are run with:

```
$ mvn test
```

AWS component using tests, including running a local DynamoDB instance, can be run (does not 
run basic tests) with:

```
$ mvn test -DawsLocalTests
```

Both sets of tests can be run together with:

```
$ mvn test -DallTests

## Deploying


### 1. Setup Environment (first time only)

```$ setup-deploy.sh```

This installs the correct ruby dependencies

### 2. Build Deployable Jar

```$ deploy/build.rb```

It's a light wrapper around maven that adds properties file we can use to set connection information


### 3. Deploy jar to aws

```$ deploy/run.rb```
