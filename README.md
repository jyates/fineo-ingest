# Lamba Functions

Implementations of lambda functions for fineo.

## Building

Standard maven stuff here. Requirements:
 * Maven 3.X
 * Java 8

## Packaging for deployment

A deployable package can be built with:

```
 $ ./build.sh <options>
```

It's a light wrapper around maven to add a properties file we can use to set connection information.
