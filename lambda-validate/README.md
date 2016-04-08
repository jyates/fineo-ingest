# Lambda Validation

Validate the deployed lambda functions. 

To run the test, you run a mvn verify with the validation option:

```
 $ mvn clean verify -Dfailsafe.skip.validation=false
```

## Test Parameters

Since the functions are running on AWS you need to provide some information to access AWS. The 
necessary parameters are:

 * ```-Dcredentials```
  * Full path to the YAML format credentials file on the local FS. If this is not provided the test attempts to use the credentials in the ```[aws-testing]``` profile credentials(~/.aws/credentials)
