package io.fineo.stream.processing.e2e;

import com.beust.jcommander.JCommander;
import com.google.inject.Module;
import io.fineo.etl.FineoProperties;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.dynamo.DynamoTestConfiguratorModule;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.stream.processing.e2e.command.BaseCommand;
import io.fineo.stream.processing.e2e.command.InMemoryExecCommand;
import io.fineo.stream.processing.e2e.module.FakeAwsCredentialsModule;
import io.fineo.stream.processing.e2e.options.FirehoseOutput;
import io.fineo.stream.processing.e2e.options.JsonArgument;
import io.fineo.stream.processing.e2e.options.LocalOptions;
import io.fineo.stream.processing.e2e.options.SkipValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class EndToEndWrapper {


  private static final Logger LOG = LoggerFactory.getLogger(EndToEndWrapper.class);

  public static void main(String[] args) throws Exception {
    SkipValidation validate = new SkipValidation();
    JsonArgument json = new JsonArgument();
    LocalOptions local = new LocalOptions();
    FirehoseOutput output = new FirehoseOutput();
    JCommander jc = new JCommander(new Object[]{local, json, output, validate});
    jc.addCommand("local", new InMemoryExecCommand(output, validate));

    jc.parse(args);

    List<Map<String, Object>> events = json.get();
    LOG.debug("Writing events: {}", events);
    List<Module> schemaStore = getSchemaStoreModules(local);

    String cmd = jc.getParsedCommand();
    BaseCommand command = (BaseCommand) jc.getCommands().get(cmd).getObjects().get(0);
    command.run(schemaStore, events);
  }

  private static List<Module> getSchemaStoreModules(LocalOptions store) {
    List<Module> modules = new ArrayList<>();
    // support for local dynamo
    modules.add(new FakeAwsCredentialsModule());
    modules.add(new DynamoTestConfiguratorModule());
    modules.add(new DynamoModule());
    modules.add(new SchemaStoreModule());

    // properties to support the build
    Properties props = new Properties();
    props.setProperty(FineoProperties.DYNAMO_URL_FOR_TESTING,
      "http://" + store.host + ":" + store.port);
    props.setProperty(FineoProperties.DYNAMO_INGEST_TABLE_PREFIX, store.ingestTablePrefix);
    props.setProperty(FineoProperties.DYNAMO_SCHEMA_STORE_TABLE, store.schemaTable);
    props.setProperty(FineoProperties.DYNAMO_READ_LIMIT, "1");
    props.setProperty(FineoProperties.DYNAMO_WRITE_LIMIT, "1");
    props.setProperty(FineoProperties.DYNAMO_RETRIES, "3");
    props.setProperty(FineoProperties.DYNAMO_TABLE_MANAGER_CACHE_TIME, "3600000");

    modules.add(new PropertiesModule(props));

    return modules;
  }
}
