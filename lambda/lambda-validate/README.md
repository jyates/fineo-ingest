# Lambda Validation

Validate the deployed lambda functions. To run the test, you just kick off a standard maven test 
suite:

```
 $ mvn clean test [options]
```

## Test Parameters

Since the functions are running on AWS you need to provide some information to access AWS. The 
necessary parameters are:

 * ```-Dcredentials```
  * Full path to the YAML format credentials file on the local FS. If this is not provided 
  attempts to use the credentials in the ```[aws-testing]``` profile credentials(~/.aws/credentials)
