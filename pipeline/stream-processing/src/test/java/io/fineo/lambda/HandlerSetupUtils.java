package io.fineo.lambda;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import io.fineo.lambda.configure.NullableNamedInstanceModule;
import io.fineo.lambda.configure.firehose.FirehoseModule;
import io.fineo.lambda.firehose.FirehoseBatchWriter;
import io.fineo.lambda.firehose.IFirehoseBatchWriter;
import io.fineo.lambda.handle.MalformedEventToJson;

import java.time.Clock;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public final class HandlerSetupUtils {
  private HandlerSetupUtils() {
  }

  public static List<Module> getBasicTestModules(FirehoseBatchWriter records,
    FirehoseBatchWriter malformed,
    FirehoseBatchWriter error) {
    Module rModule =
      new NullableNamedInstanceModule<>(FirehoseModule.FIREHOSE_ARCHIVE_STREAM, records,
        IFirehoseBatchWriter.class);
    Module mModule =
      new NullableNamedInstanceModule<>(FirehoseModule.FIREHOSE_MALFORMED_RECORDS_STREAM,
        malformed,
        IFirehoseBatchWriter.class);
    Module eModule =
      new NullableNamedInstanceModule<>(FirehoseModule.FIREHOSE_COMMIT_ERROR_STREAM, error,
        IFirehoseBatchWriter.class);
    return newArrayList(rModule, mModule, eModule,
      clockModule(),
      MalformedEventToJson.getModule()
    );
  }

  public static Module clockModule() {
    return instance(Clock.systemUTC(), Clock.class);
  }

  public static <T> Module instance(T instance, Class<T> clazz) {
    return new AbstractModule() {

      @Override
      protected void configure() {
        bind(clazz).toInstance(instance);
      }
    };
  }
}
