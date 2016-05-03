# Deploy

Ruby scripts to enable easy deployment of the ingest modules. Each major component is broken out
into a set of deployable artifacts. Currently, no guarantees are made around idempotency, so
use *with caution*

## Requirements

RVM - the ruby version manager

### 1. Setup Environment (first time only)

```$ setup-deploy.sh```

This installs the correct ruby dependencies

### 2. Build Deployable Jar(s)

```$ deploy/build-jar/lib/build-jar.rb```

Builds the properties that each jar needs and then sets those properties in each of the desired deployment targets. Also allows you to 
override the properties with command line options.

### 3. Deploy jar to aws

```$ deploy/deploy-processing/lib/deploy-processing.rb```

Does the actual deployment. You can deploy all the stages, functions, and jars at once, or select
a subset to deploy. Use ```-h``` to find what you can select.

### Deployment Validation

#### Stream processing

A test version can be deployed and tested with the simple command:

```$ test-streaming.sh```

This will build a deployable lambda jar with test versions, deploy it to AWS and then run a validation test against the deployment and then cleanup all the test resources (kinesis pipes, lambda event sources, firehoses, dynamo tables, etc.) used for the validation.

However, the lambda function is **not deleted**, so you will have to publish a new version with a new configuration (see above).  
