package io.fineo.lambda.e2e.resources.manager;

import com.google.inject.Injector;
import io.fineo.lambda.util.run.FutureWaiter;

import java.io.IOException;

public interface IResource {

  void init(Injector injector) throws IOException;

  void cleanup(FutureWaiter waiter);
}
