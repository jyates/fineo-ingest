package io.fineo.stream.processing.e2e.command;

import com.beust.jcommander.Parameters;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.fineo.lambda.e2e.EndToEndTestRunner;
import io.fineo.lambda.e2e.ITEndToEndLambdaLocal;
import io.fineo.schema.store.SchemaStore;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;


@Parameters(commandNames = "local",
            commandDescription = "Run the ingest against a local target")
public class LocalExecCommand extends BaseCommand{

  @Override
  public void run(List<Module> baseModules, Map<String, Object> event) throws Exception {
    // setup the different lambda functions
    SchemaStore store = getSchemaStore(baseModules);
    ITEndToEndLambdaLocal.TestState state = ITEndToEndLambdaLocal.prepareTest(store);
    EndToEndTestRunner runner = state.getRunner();
    runner.setup();
    runner.send(event);
    runner.validate();
    state.getRunner().cleanup();
  }

  private SchemaStore getSchemaStore(List<Module> baseModules) {
    List<Module> schema = newArrayList(baseModules);
    Injector guice = Guice.createInjector(schema);
    return guice.getInstance(SchemaStore.class);
  }
}
