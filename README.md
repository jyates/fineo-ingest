# Lamba Functions

Implementations of lambda functions for fineo.

## Building

Standard maven stuff here. Requirements:
 * Maven 3.X
 * Java 8

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

## Packaging for deployment

A deployable package can be built with:

```
 $ ./build.rb <options>
```

It's a light wrapper around maven to add a properties file we can use to set connection information.
