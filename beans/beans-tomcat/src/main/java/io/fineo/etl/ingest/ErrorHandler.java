package io.fineo.etl.ingest;

import com.google.inject.BindingAnnotation;
import io.fineo.lambda.LambdaClientProperties;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;

/**
 * Handler for events that we cannot process successfully
 */
@BindingAnnotation
@Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
public @interface  ErrorHandler {

  LambdaClientProperties.StreamType type();
}
